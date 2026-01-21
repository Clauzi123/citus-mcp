package safety

// Limiter is a placeholder rate limiter.
type Limiter struct {}

func NewLimiter() *Limiter { return &Limiter{} }

func (l *Limiter) Allow(action string) bool { return true }
