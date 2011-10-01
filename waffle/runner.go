package waffle

import (
	"os"
)

type Runner struct {
	master bool
}

func (r *Runner) Run() os.Error {
	return nil
}
