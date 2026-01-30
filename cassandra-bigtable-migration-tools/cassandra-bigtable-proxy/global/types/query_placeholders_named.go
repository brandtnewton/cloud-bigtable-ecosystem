package types

import "fmt"

type NamedQueryParameters struct {
	params map[Placeholder]*PlaceholderMetadata
}

func (n *NamedQueryParameters) GetMetadata(placeholder Placeholder) (*PlaceholderMetadata, error) {
	metadata, ok := n.params[placeholder]
	if !ok {
		return nil, fmt.Errorf("missing named parameter '%s'", placeholder)
	}
	return metadata, nil
}

func (n *NamedQueryParameters) Params() map[Placeholder]*PlaceholderMetadata {
	return n.params
}

func NewNamedQueryParameters(params map[Placeholder]*PlaceholderMetadata) IQueryParameters {
	return &NamedQueryParameters{params: params}
}

func (n *NamedQueryParameters) Count() int {
	return len(n.params)
}
