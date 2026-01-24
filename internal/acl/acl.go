package acl

import (
	"sync"
)

type User struct {
	Name       string
	Password   string // Plain text for simplicity, or hash? Challenge uses plain.
	Enabled    bool
	Categories []string // e.g. "+@all", "-@dangerous"
	Commands   []string // e.g. "+get", "-set"
	Keys       []string // e.g. "~*"
	// For this challenge, we assume default user has full access if authenticated.
}

type Engine struct {
	users map[string]*User
	mu    sync.RWMutex
}

func NewEngine(requirePass string) *Engine {
	e := &Engine{
		users: make(map[string]*User),
	}

	// Create default user
	defaultUser := &User{
		Name:    "default",
		Enabled: true,
		// If requirePass is empty, password is "nopass" (empty string matches nothing usually, but here implies no auth needed)
		// Actually if requirePass is "", default user has no password.
		Password: requirePass,
	}
	e.users["default"] = defaultUser
	return e
}

func (e *Engine) GetUser(name string) (*User, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	u, ok := e.users[name]
	return u, ok
}

func (e *Engine) Authenticate(username, password string) (*User, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	u, ok := e.users[username]
	if !ok {
		return nil, false
	}
	if !u.Enabled {
		return nil, false
	}

	// Check password
	// If User has no password set (and we aren't using requirepass logic strictness), what happens?
	// With requirepass, default user HAS a password.
	// If u.Password is empty, does it mean nopass?
	// Redis logic: if password is set, match it.
	if u.Password != "" && u.Password != password {
		return nil, false
	}
	// If u.Password is empty, any password works? Or no password needed?
	// Usually "nopass" means auth succeeds without password.
	
	return u, true
}

func (e *Engine) ListUsers() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	names := make([]string, 0, len(e.users))
	for k := range e.users {
		names = append(names, k)
	}
	return names
}
