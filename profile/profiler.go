package profile

import "strings"

// hasLeadingZeros checks if a valid integer value contains leading zeros.
// This is often an indicator that this is not an integer, but an identfier.
func hasLeadingZeros(s string) bool {
	if s == "" {
		return false
	}

	return s[0] == '0'
}

type profiler struct {
	Config  *Config
	Count   int64
	Include map[string]struct{}
	Exclude map[string]struct{}
	Fields  map[string]*profilerField
}

// Profiler is an interface for profiling data.
type Profiler interface {
	// Increment the record count.
	Incr()

	// Record records a field-value pair to the profile of an unknown type.
	// The value must be a encoded as a string and will be parsed in a variety
	// of ways to detect the type.
	Record(field string, raw string)

	// RecordType recorsd a field-value pair with a known type.
	RecordType(field string, value interface{}, typ ValueType)

	// Profile returns the profile.
	Profile() *Profile
}

type Config struct {
	// Include are the fields to explicitly include.
	Include []string

	// Exclude are the fields to explicitly exclude.
	Exclude []string
}

func (p *profiler) Incr() {
	p.Count++
}

// field returns the field profile if it should be profiled.
func (p *profiler) field(n string) (*profilerField, bool) {
	n = strings.ToLower(n)

	if _, ok := p.Exclude[n]; ok {
		return nil, false
	}

	if len(p.Include) > 0 {
		if _, ok := p.Include[n]; !ok {
			return nil, false
		}
	}

	// Initialize and get field profile.
	f, ok := p.Fields[n]
	if !ok {
		f = newProfilerField(n)
		p.Fields[n] = f
	}

	return f, true
}

func (p *profiler) Profile() *Profile {
	r := NewProfile()
	r.RecordCount = p.Count

	for k, f := range p.Fields {
		r.Fields[k] = f.Field()
	}

	return r
}

func (p *profiler) Record(n string, v string) {
	f, ok := p.field(n)
	if !ok {
		return
	}

	// Still in the unique state.
	if f.Unique {
		// Duplicate value.
		if _, ok := f.Values[v]; ok {
			f.Unique = false
			f.Values = nil
		} else {
			f.Values[v] = struct{}{}
		}
	}

	// Short circuit. Already most general type.
	if _, ok := f.Types[StringType]; ok {
		return
	}

	if _, ok := ParseInt(v); ok {
		if !f.LeadingZeros && hasLeadingZeros(v) {
			f.LeadingZeros = true
		}

		f.Types[IntType] = struct{}{}
		return
	}

	if _, ok := ParseFloat(v); ok {
		f.Types[FloatType] = struct{}{}
		return
	}

	if _, ok := ParseBool(v); ok {
		f.Types[BoolType] = struct{}{}
		return
	}

	if _, ok := ParseDate(v); ok {
		f.Types[DateType] = struct{}{}
		return
	}

	if _, ok := ParseDateTime(v); ok {
		f.Types[DateTimeType] = struct{}{}
		return
	}

	f.Types[StringType] = struct{}{}
}

func (p *profiler) RecordType(n string, v interface{}, t ValueType) {
	f, ok := p.field(n)
	if !ok {
		return
	}

	f.Types[t] = struct{}{}
}

// Field stores aggregation information and statistics for a field.
type profilerField struct {
	Name         string
	Types        map[ValueType]struct{}
	Values       map[string]struct{}
	Unique       bool
	LeadingZeros bool
}

func (p *profilerField) Field() *Field {
	_, nullable := p.Types[NullType]

	f := Field{
		Name:         p.Name,
		Type:         p.Type(),
		Nullable:     nullable,
		Unique:       p.Unique,
		LeadingZeros: p.LeadingZeros,
	}

	return &f
}

// Type returns the most specific type this field satisfies.
func (f *profilerField) Type() ValueType {
	if f.LeadingZeros {
		return StringType
	}

	var g ValueType

	for t := range f.Types {
		if g == UnknownType {
			g = t
		} else {
			g = GeneralizeType(t, g)
		}
	}

	return g
}

func newProfilerField(name string) *profilerField {
	return &profilerField{
		Name:   name,
		Types:  make(map[ValueType]struct{}),
		Values: make(map[string]struct{}),
		Unique: true,
	}
}

func NewProfiler(c *Config) Profiler {
	if c == nil {
		c = &Config{}
	}

	p := &profiler{
		Config: c,
		Fields: make(map[string]*profilerField),
	}

	if len(p.Config.Exclude) > 0 {
		p.Exclude = make(map[string]struct{})

		for _, f := range p.Config.Exclude {
			p.Exclude[strings.ToLower(f)] = struct{}{}
		}
	}

	if len(p.Config.Include) > 0 {
		p.Include = make(map[string]struct{})

		for _, f := range p.Config.Include {
			p.Include[strings.ToLower(f)] = struct{}{}
		}
	}

	return p
}
