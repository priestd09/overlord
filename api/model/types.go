package model

// Job is the json-encodable struct
type Job struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

// Cluster is the special struct in model
type Cluster struct {
	Name   string   `json:"name"`
	Appids []string `json:"appids"`
	State  string   `json:"state"`

	CacheType string `json:"cache_type"`
	Spec      string `json:"spec" validate:"required"`
	Version   string `json:"version" validate:"required"`
	Number    int    `json:"number" validate:"required"`

	Instances []*Instance `json:"instances"`
}

// Instance is the struct for each cache
type Instance struct {
	IP    string `json:"ip"`
	Port  string `json:"port"`
	State string `json:"state"`
}
