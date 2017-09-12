package profile

// Field stores aggregation information and statistics for a field.
type Field struct {
	// Name of this field.
	Name string `json:"name"`

	// Index of the field in tabular sources.
	Index int `json:"index"`

	// Inferred type of the field. All candidates types are in the
	// type counts array.
	Type ValueType `json:"type"`

	// True if the field contains null values.
	Nullable bool `json:"nullable"`

	// True if the field contains empty strings.
	Missing bool `json:"missing"`

	// True if all values are unique.
	Unique bool `json:"unique"`

	// If true, at least one value has been detected to have a leading zero.
	LeadingZeros bool `json:"leading_zeros"`
}

type Profile struct {
	// Total number os records processed.
	RecordCount int64 `json:"record_count"`

	// Flat set of fields that were profiled.
	Fields map[string]*Field `json:"fields"`
}

func NewProfile() *Profile {
	return &Profile{
		Fields: make(map[string]*Field),
	}
}
