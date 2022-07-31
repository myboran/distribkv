package web

import (
	"distribkv/db"
	"fmt"
	"net/http"
)

// Server contains HTTP method handlers to be used for the database.
type Server struct {
	db *db.Database
}

// NewServer creates a new instance with HTTP handlers to be used to get and set values.
func NewServer(db *db.Database) *Server {
	return &Server{db: db}
}

// GetHandler handlers read requests from the database.
func (s *Server) GetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")

	value, err := s.db.GetKey(key)
	if err != nil {
		fmt.Fprintf(w, "error = %v", err)
		return
	}
	fmt.Fprintf(w, "value = %q", value)

}

// SetHandler handlers write requests from the database.
func (s *Server) SetHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	key := r.Form.Get("key")
	value := r.Form.Get("value")

	err := s.db.SetKey(key, []byte(value))

	fmt.Fprintf(w, "Error = %v", err)
}

// ListenAndServe starts to accept requests.
func (s *Server) ListenAndServe(addr string) error {
	return http.ListenAndServe(addr, nil)
}
