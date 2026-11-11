package betfair

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
)

const DefaultMaxMessageSize = 1 << 20

var ErrMessageTooLarge = errors.New("betfair stream message exceeds maximum size")

type Parser struct {
	reader         *bufio.Reader
	maxMessageSize int
}

func NewParser(r io.Reader, maxMessageSize int) *Parser {
	if maxMessageSize <= 0 {
		maxMessageSize = DefaultMaxMessageSize
	}
	return &Parser{
		reader:         bufio.NewReader(r),
		maxMessageSize: maxMessageSize,
	}
}

func (p *Parser) Next(ctx context.Context) (Message, error) {
	raw, err := p.nextRaw(ctx)
	if err != nil {
		return Message{}, err
	}
	msg, err := DecodeMessage(raw)
	if err != nil {
		return Message{}, err
	}
	return msg, nil
}

func (p *Parser) nextRaw(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	buf := make([]byte, 0, 512)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		b, err := p.reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) && len(buf) == 0 {
				return nil, io.EOF
			}
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("read betfair stream message: unterminated CRLF-delimited message")
			}
			return nil, fmt.Errorf("read betfair stream message: %w", err)
		}

		if b == '\n' {
			if len(buf) == 0 || buf[len(buf)-1] != '\r' {
				return nil, fmt.Errorf("read betfair stream message: expected CRLF delimiter")
			}
			raw := buf[:len(buf)-1]
			if len(raw) == 0 {
				return nil, fmt.Errorf("read betfair stream message: empty message")
			}
			if len(raw) > p.maxMessageSize {
				return nil, ErrMessageTooLarge
			}
			return append([]byte(nil), raw...), nil
		}

		buf = append(buf, b)
		if len(buf) > p.maxMessageSize+1 {
			return nil, ErrMessageTooLarge
		}
	}
}

func ReadAll(ctx context.Context, r io.Reader, maxMessageSize int) ([]Message, error) {
	parser := NewParser(r, maxMessageSize)
	messages := []Message{}
	for {
		msg, err := parser.Next(ctx)
		if errors.Is(err, io.EOF) {
			return messages, nil
		}
		if err != nil {
			return messages, err
		}
		messages = append(messages, msg)
	}
}
