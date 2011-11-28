package waffle

const (
	WAFFLE_VERSION            = "0.0.0"
	DEFAULT_HEARTBEAT_TIMEOUT = 8 * 1e9
)

const (
	PHASE_REGISTER = iota
	PHASE_LOAD_DATA
	PHASE_DISTRIBUTE_VERTICES
	PHASE_LOAD_PERSISTED // load from persistence
	PHASE_STEP_PREPARE
	PHASE_SUPERSTEP
	PHASE_WRITE_RESULTS
	PHASE_RECOVER
	PHASE_FAILURE
)

type WorkerError struct {
	WorkerId    string
	ErrorId     int
	ErrorString string
}

func (e *WorkerError) Error() string {
	return e.ErrorString
}

type RegistrationTimeoutError struct {
	message string
}

func (e *RegistrationTimeoutError) Error() string {
	return e.message
}
