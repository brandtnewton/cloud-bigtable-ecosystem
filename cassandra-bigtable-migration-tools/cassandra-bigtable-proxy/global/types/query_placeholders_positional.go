package types

import "fmt"

type PositionalQueryParameters struct {
	ordered []*PlaceholderMetadata
}

func (p *PositionalQueryParameters) GetMetadata(placeholder Placeholder) (*PlaceholderMetadata, error) {
	for _, metadata := range p.ordered {
		if metadata.Key == placeholder {
			return metadata, nil
		}
	}
	return nil, fmt.Errorf("no query param for placeholder '%s'", placeholder)
}

func (p *PositionalQueryParameters) Count() int {
	return len(p.ordered)
}

func (p *PositionalQueryParameters) Ordered() []*PlaceholderMetadata {
	return p.ordered
}

func NewPositionalQP(ordered []*PlaceholderMetadata) IQueryParameters {
	return &PositionalQueryParameters{ordered: ordered}
}
