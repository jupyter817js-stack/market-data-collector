// Command clob-approve sends an ERC-20 approve transaction so the
// Polymarket CTF Exchange V2 contract can spend our USDC on settlement.
// This is the one-time onchain step required for an EOA-mode (Wallet
// mode A) account before /balance-allowance shows a non-zero allowance.
//
// SAFETY POSTURE:
//   - Default behaviour does NOT broadcast: the cmd builds, signs, and
//     prints the raw tx hex + decoded calldata, then exits. Useful to
//     verify spender/amount/gas without paying.
//   - -really-send broadcasts the signed tx via eth_sendRawTransaction.
//     Once mined, allowance becomes effective on the CTF Exchange.
//
// USAGE:
//
//	# Dry-run (default): print signed tx hex, no broadcast
//	go run ./cmd/clob-approve -spender 0xE111180000d2663C0091e4f400237545B87B996B -amount max
//
//	# Actually send (REQUIRES POL gas in the EOA, ~0.05 POL is plenty)
//	go run ./cmd/clob-approve -spender 0xE111180000d2663C0091e4f400237545B87B996B -amount max -really-send
//
// CONFIG (env):
//
//	POLYMARKET_SIGNER_PK      EOA private key (hex; 0x optional)
//	POLYGON_RPC_URL           default https://polygon-rpc.com
//	POLYMARKET_USDC_ADDRESS   default 0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359 (native USDC)
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/your-org/pol-odds-collector/internal/config"
)

const (
	defaultRPC         = "https://polygon-rpc.com"
	usdcNativePolygon  = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
	usdcBridgedPolygon = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
	ctfExchangeV2      = "0xE111180000d2663C0091e4f400237545B87B996B"
	polygonChainID     = 137
)

// erc20ApproveSelector = bytes4(keccak256("approve(address,uint256)"))
const erc20ApproveSelector = "095ea7b3"

func main() {
	var (
		spender     = flag.String("spender", ctfExchangeV2, "spender address (default CTF Exchange V2)")
		amountStr   = flag.String("amount", "max", "approval amount (decimal USDC like '50' OR 'max' for unlimited)")
		tokenAddr   = flag.String("token", "", "ERC-20 token address (default env POLYMARKET_USDC_ADDRESS or native USDC)")
		rpcURL      = flag.String("rpc-url", "", "Polygon RPC URL (default env POLYGON_RPC_URL or https://polygon-rpc.com)")
		gasGwei     = flag.Float64("gas-gwei", 0, "max fee per gas (gwei); 0 = use eth_gasPrice * 1.25")
		gasLimit    = flag.Uint64("gas-limit", 80000, "gas limit (60k typical for ERC-20 approve)")
		reallySend  = flag.Bool("really-send", false, "REQUIRED to broadcast the signed tx via eth_sendRawTransaction")
	)
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	_, _ = config.Load()

	pk := strings.TrimSpace(os.Getenv("POLYMARKET_SIGNER_PK"))
	if pk == "" {
		pk = strings.TrimSpace(os.Getenv("POLYMARKET_PRIVATE_KEY"))
	}
	if pk == "" {
		die("POLYMARKET_SIGNER_PK env required (EOA private key, hex)")
	}
	pk = strings.TrimPrefix(pk, "0x")
	ecdsaKey, err := crypto.HexToECDSA(pk)
	if err != nil {
		die("private key parse: " + err.Error())
	}
	eoa := crypto.PubkeyToAddress(ecdsaKey.PublicKey)

	if !common.IsHexAddress(*spender) {
		die("--spender not a valid address: " + *spender)
	}
	spenderAddr := common.HexToAddress(*spender)

	tok := strings.TrimSpace(*tokenAddr)
	if tok == "" {
		tok = strings.TrimSpace(os.Getenv("POLYMARKET_USDC_ADDRESS"))
	}
	if tok == "" {
		tok = usdcNativePolygon
	}
	if !common.IsHexAddress(tok) {
		die("--token not a valid address: " + tok)
	}
	tokenAddrParsed := common.HexToAddress(tok)

	// amount: "max" = 2^256-1, else decimal USDC * 10^6
	var amount *big.Int
	if strings.EqualFold(*amountStr, "max") {
		amount = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	} else {
		// parse as float to allow "5.5", then *1e6 (USDC has 6 decimals)
		var f float64
		if _, perr := fmt.Sscanf(*amountStr, "%f", &f); perr != nil {
			die("--amount must be 'max' or decimal: " + perr.Error())
		}
		if f <= 0 {
			die("--amount must be > 0")
		}
		// f * 1e6 -> integer
		micro := new(big.Float).Mul(big.NewFloat(f), big.NewFloat(1e6))
		amount = new(big.Int)
		micro.Int(amount)
	}

	rpc := strings.TrimSpace(*rpcURL)
	if rpc == "" {
		rpc = strings.TrimSpace(os.Getenv("POLYGON_RPC_URL"))
	}
	if rpc == "" {
		rpc = defaultRPC
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cli, err := ethclient.DialContext(ctx, rpc)
	if err != nil {
		die("ethclient.Dial: " + err.Error())
	}
	defer cli.Close()

	slog.Info("clob-approve plan",
		"eoa", eoa.Hex(),
		"token", tokenAddrParsed.Hex(),
		"spender", spenderAddr.Hex(),
		"amount", amountStr,
		"rpc", rpc,
	)

	// Fetch nonce + suggested fees.
	nonce, err := cli.PendingNonceAt(ctx, eoa)
	if err != nil {
		die("PendingNonceAt: " + err.Error())
	}
	gasPrice, err := cli.SuggestGasPrice(ctx)
	if err != nil {
		die("SuggestGasPrice: " + err.Error())
	}
	if *gasGwei > 0 {
		gasPrice = new(big.Int).Mul(big.NewInt(int64(*gasGwei*1e9)), big.NewInt(1))
	} else {
		// 25% headroom
		gasPrice = new(big.Int).Div(new(big.Int).Mul(gasPrice, big.NewInt(125)), big.NewInt(100))
	}

	// Native POL balance for sanity.
	polBal, err := cli.BalanceAt(ctx, eoa, nil)
	if err != nil {
		die("BalanceAt: " + err.Error())
	}
	polEth := new(big.Float).Quo(new(big.Float).SetInt(polBal), big.NewFloat(1e18))
	estGasCost := new(big.Int).Mul(gasPrice, big.NewInt(int64(*gasLimit)))
	estGasCostEth := new(big.Float).Quo(new(big.Float).SetInt(estGasCost), big.NewFloat(1e18))
	slog.Info("network state",
		"nonce", nonce,
		"gas_price_gwei", fmtBigFloat(new(big.Float).Quo(new(big.Float).SetInt(gasPrice), big.NewFloat(1e9))),
		"gas_limit", *gasLimit,
		"est_gas_cost_POL", fmtBigFloat(estGasCostEth),
		"pol_balance", fmtBigFloat(polEth),
	)
	if polBal.Cmp(estGasCost) < 0 {
		slog.Warn("INSUFFICIENT POL FOR GAS",
			"have_POL", fmtBigFloat(polEth),
			"need_POL", fmtBigFloat(estGasCostEth),
		)
	}

	// Build calldata: approve(spender, amount).
	data := buildApproveCalldata(spenderAddr, amount)
	slog.Info("calldata",
		"selector", "0x"+erc20ApproveSelector,
		"hex", "0x"+hex.EncodeToString(data),
	)

	// Build + sign legacy tx (works regardless of EIP-1559 / EIP-7702 state).
	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       &tokenAddrParsed,
		Value:    big.NewInt(0),
		Gas:      *gasLimit,
		GasPrice: gasPrice,
		Data:     data,
	})
	signer := types.NewEIP155Signer(big.NewInt(polygonChainID))
	signedTx, err := types.SignTx(tx, signer, ecdsaKey)
	if err != nil {
		die("SignTx: " + err.Error())
	}
	raw, err := signedTx.MarshalBinary()
	if err != nil {
		die("MarshalBinary: " + err.Error())
	}
	rawHex := "0x" + hex.EncodeToString(raw)
	slog.Info("signed tx",
		"tx_hash_pre_broadcast", signedTx.Hash().Hex(),
		"raw_hex_len", len(rawHex),
	)

	if !*reallySend {
		slog.Info("dry-run: not broadcasting (pass -really-send to broadcast)")
		fmt.Println()
		fmt.Println("Signed raw tx hex (re-sendable):")
		fmt.Println(rawHex)
		return
	}

	if err := cli.SendTransaction(ctx, signedTx); err != nil {
		die("SendTransaction: " + err.Error())
	}
	slog.Info("BROADCASTED — waiting for receipt (max 90s)",
		"tx_hash", signedTx.Hash().Hex(),
		"polygonscan", "https://polygonscan.com/tx/"+signedTx.Hash().Hex(),
	)

	// Poll for receipt.
	recCtx, recCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer recCancel()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		receipt, err := cli.TransactionReceipt(recCtx, signedTx.Hash())
		if err == nil && receipt != nil {
			if receipt.Status == types.ReceiptStatusSuccessful {
				slog.Info("✅ APPROVE CONFIRMED",
					"tx_hash", signedTx.Hash().Hex(),
					"block", receipt.BlockNumber.String(),
					"gas_used", receipt.GasUsed,
				)
			} else {
				slog.Error("❌ APPROVE REVERTED",
					"tx_hash", signedTx.Hash().Hex(),
					"block", receipt.BlockNumber.String(),
				)
			}
			return
		}
		time.Sleep(3 * time.Second)
	}
	slog.Warn("receipt not yet available — check Polygonscan",
		"tx_hash", signedTx.Hash().Hex(),
	)
}

// buildApproveCalldata returns the 4-byte selector + 32-byte spender +
// 32-byte amount = 68 bytes total.
func buildApproveCalldata(spender common.Address, amount *big.Int) []byte {
	sel, _ := hex.DecodeString(erc20ApproveSelector)
	out := make([]byte, 0, 4+32+32)
	out = append(out, sel...)
	// spender padded to 32 bytes (12 zero bytes + 20 address bytes)
	pad := make([]byte, 12)
	out = append(out, pad...)
	out = append(out, spender.Bytes()...)
	// amount left-padded to 32 bytes
	amtBytes := amount.Bytes()
	if len(amtBytes) >= 32 {
		out = append(out, amtBytes[len(amtBytes)-32:]...)
	} else {
		amtPad := make([]byte, 32-len(amtBytes))
		out = append(out, amtPad...)
		out = append(out, amtBytes...)
	}
	return out
}

func fmtBigFloat(f *big.Float) string {
	s := f.Text('f', 6)
	return s
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
