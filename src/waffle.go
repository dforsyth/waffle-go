package waffle

const (
	WAFFLE_VERSION                    = "0.0.0"
	DEFAULT_HEARTBEAT_INTERVAL        = 8 * 1e9
	DEFAULT_MAX_STEPS                 = 0
	DEFAULT_MIN_PARTITIONS_PER_WORKER = 1
	DEFAULT_MIN_WORKERS               = 1
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

type PhaseError struct {
	phase         int
	failedWorkers []error
	phaseErrors   []error
}

func (e *PhaseError) Error() string {
	rval := "PhaseError:\n"
	for _, error := range e.failedWorkers {
		rval += error.Error() + "\n"
	}
	for _, error := range e.phaseErrors {
		rval += error.Error() + "\n"
	}
	return rval
}

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
