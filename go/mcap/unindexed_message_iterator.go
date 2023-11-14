package mcap

import (
	"fmt"
)

type unindexedMessageIterator struct {
	lexer    *Lexer
	schemas  []*Schema
	channels []*Channel
	topics   map[string]bool
	start    uint64
	end      uint64

	metadataCallback func(*Metadata) error
}

func (it *unindexedMessageIterator) NextPreallocated(schema *Schema, channel *Channel, msg *Message, buf []byte) error {
	for {
		tokenType, record, err := it.lexer.Next(buf)
		if err != nil {
			return err
		}
		switch tokenType {
		case TokenSchema:
			schema, err := ParseSchema(record)
			if err != nil {
				return fmt.Errorf("failed to parse schema: %w", err)
			}
			if len(it.schemas) > int(schema.ID)+1 {
				schemas := make([]*Schema, (1+schema.ID)*2)
				copy(schemas, it.schemas)
				it.schemas = schemas
			}
			if it.schemas[schema.ID] == nil {
				it.schemas[schema.ID] = schema
			}
		case TokenChannel:
			channel, err := ParseChannel(record)
			if err != nil {
				return fmt.Errorf("failed to parse channel info: %w", err)
			}
			if len(it.channels) > int(channel.ID)+1 {
				channels := make([]*Channel, (1+channel.ID)*2)
				copy(channels, it.channels)
				it.channels = channels
			}
			if it.channels[channel.ID] == nil {
				if len(it.topics) == 0 || it.topics[channel.Topic] {
					it.channels[channel.ID] = channel
				}
			}
		case TokenMessage:
			err = ParseMessageInto(msg, record)
			if err != nil {
				return err
			}
			if it.channels[msg.ChannelID] == nil {
				// skip messages on channels we don't know about. Note that if
				// an unindexed reader encounters a message it would be
				// interested in, but has not yet encountered the corresponding
				// channel ID, it has no option but to skip.
				continue
			}
			if msg.LogTime >= it.start && msg.LogTime < it.end {
				*channel = *it.channels[msg.ChannelID]
				*schema = *it.schemas[channel.SchemaID]
				return nil
			}
		case TokenMetadata:
			if it.metadataCallback != nil {
				metadata, err := ParseMetadata(record)
				if err != nil {
					return fmt.Errorf("failed to parse metadata: %w", err)
				}
				err = it.metadataCallback(metadata)
				if err != nil {
					return err
				}
			}
			// we don't emit metadata from the reader, so continue onward
			continue
		default:
			// skip all other tokens
		}
	}
}

func (it *unindexedMessageIterator) Next(p []byte) (*Schema, *Channel, *Message, error) {
	for {
		tokenType, record, err := it.lexer.Next(p)
		if err != nil {
			return nil, nil, nil, err
		}
		switch tokenType {
		case TokenSchema:
			schema, err := ParseSchema(record)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to parse schema: %w", err)
			}
			if len(it.schemas) > int(schema.ID)+1 {
				schemas := make([]*Schema, (1+schema.ID)*2)
				copy(schemas, it.schemas)
				it.schemas = schemas
			}
			if it.schemas[schema.ID] == nil {
				it.schemas[schema.ID] = schema
			}
		case TokenChannel:
			channel, err := ParseChannel(record)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to parse channel info: %w", err)
			}
			if len(it.channels) > int(channel.ID)+1 {
				channels := make([]*Channel, (1+channel.ID)*2)
				copy(channels, it.channels)
				it.channels = channels
			}
			if it.channels[channel.ID] == nil {
				if len(it.topics) == 0 || it.topics[channel.Topic] {
					it.channels[channel.ID] = channel
				}
			}
		case TokenMessage:
			message, err := ParseMessage(record)
			if err != nil {
				return nil, nil, nil, err
			}
			if it.channels[message.ChannelID] == nil {
				// skip messages on channels we don't know about. Note that if
				// an unindexed reader encounters a message it would be
				// interested in, but has not yet encountered the corresponding
				// channel ID, it has no option but to skip.
				continue
			}
			if message.LogTime >= it.start && message.LogTime < it.end {
				channel := it.channels[message.ChannelID]
				schema := it.schemas[channel.SchemaID]
				return schema, channel, message, nil
			}
		case TokenMetadata:
			if it.metadataCallback != nil {
				metadata, err := ParseMetadata(record)
				if err != nil {
					return nil, nil, nil, fmt.Errorf("failed to parse metadata: %w", err)
				}
				err = it.metadataCallback(metadata)
				if err != nil {
					return nil, nil, nil, err
				}
			}
			// we don't emit metadata from the reader, so continue onward
			continue
		default:
			// skip all other tokens
		}
	}
}
