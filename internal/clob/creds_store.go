package clob

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// L2CredsFile is the on-disk shape of a saved L2 credential. We keep
// metadata alongside the secret so the operator can audit when it was
// issued and which address signed for it.
type L2CredsFile struct {
	Address    string    `json:"address"`        // 0x-prefixed; the EOA the creds are tied to
	IssuedAt   time.Time `json:"issued_at_utc"`  // when BootstrapL2 returned
	BaseURL    string    `json:"base_url"`       // e.g. https://clob.polymarket.com
	APIKey     string    `json:"api_key"`        // POLY-API-KEY value
	APISecret  string    `json:"api_secret"`     // base64; HMAC key (TREAT AS SECRET)
	Passphrase string    `json:"api_passphrase"` // POLY-PASSPHRASE value
}

// As returns the runtime L2Credentials view.
func (f L2CredsFile) As() L2Credentials {
	return L2Credentials{APIKey: f.APIKey, APISecret: f.APISecret, APIPassphrase: f.Passphrase}
}

// DefaultCredsPath returns the conventional file location: $XDG_CONFIG_HOME/
// pol-odds-collector/clob-creds.json (Linux/Mac) or %APPDATA%\... on
// Windows. Caller can override via flag or env.
func DefaultCredsPath() string {
	dir, err := os.UserConfigDir()
	if err != nil || dir == "" {
		dir = "."
	}
	return filepath.Join(dir, "pol-odds-collector", "clob-creds.json")
}

// SaveCreds writes file to path with 0600 permissions, via a temp+rename
// so a crash midway through never leaves a half-written file. Creates
// parent directories if absent.
func SaveCreds(path string, file L2CredsFile) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("clob: SaveCreds: empty path")
	}
	if strings.TrimSpace(file.APIKey) == "" || strings.TrimSpace(file.APISecret) == "" {
		return fmt.Errorf("clob: SaveCreds: refusing to write empty credentials")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("clob: SaveCreds mkdir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".clob-creds-*.tmp")
	if err != nil {
		return fmt.Errorf("clob: SaveCreds tmp: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		// If rename failed, scrub the temp so we don't leave the secret on
		// disk under an unexpected name.
		if _, statErr := os.Stat(tmpName); statErr == nil {
			_ = os.Remove(tmpName)
		}
	}()
	if err := os.Chmod(tmpName, 0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("clob: SaveCreds chmod tmp: %w", err)
	}
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(file); err != nil {
		tmp.Close()
		return fmt.Errorf("clob: SaveCreds encode: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("clob: SaveCreds close: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("clob: SaveCreds rename: %w", err)
	}
	return os.Chmod(path, 0o600)
}

// LoadCreds reads and decodes a credentials file. Returns an error if
// either api_key or api_secret is empty, or if the file mode is world-
// readable (defensive — refuses to use creds that may have leaked).
func LoadCreds(path string) (L2CredsFile, error) {
	st, err := os.Stat(path)
	if err != nil {
		return L2CredsFile{}, fmt.Errorf("clob: LoadCreds stat: %w", err)
	}
	// On Windows the permission bits don't have unix semantics; only check
	// on unix-like systems.
	if mode := st.Mode().Perm(); mode != 0 && mode&0o077 != 0 && !isWindows() {
		return L2CredsFile{}, fmt.Errorf("clob: LoadCreds: %s has mode %o, refusing (chmod 600 first)", path, mode)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return L2CredsFile{}, fmt.Errorf("clob: LoadCreds read: %w", err)
	}
	var f L2CredsFile
	if err := json.Unmarshal(b, &f); err != nil {
		return L2CredsFile{}, fmt.Errorf("clob: LoadCreds decode: %w", err)
	}
	if strings.TrimSpace(f.APIKey) == "" || strings.TrimSpace(f.APISecret) == "" {
		return L2CredsFile{}, fmt.Errorf("clob: LoadCreds: %s has empty api_key or api_secret", path)
	}
	return f, nil
}

func isWindows() bool { return os.PathSeparator == '\\' }
