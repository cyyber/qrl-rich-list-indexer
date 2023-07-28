package models

type Stats struct {
	Name  string `json:"name" bson:"name"`
	Value int64  `json:"value" bson:"value"`
}

// TODO (cyyber): Stats could be added in future
func NewStats(name string, value int64) *Stats {
	return &Stats{
		Name:  name,
		Value: value,
	}
}
