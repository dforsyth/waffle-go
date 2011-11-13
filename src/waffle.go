package waffle

const (
	WAFFLE_VERSION            = "0.0.0"
	DEFAULT_HEARTBEAT_TIMEOUT = 8 * 1e9
)

const (
	phaseREGISTER = iota
	phaseLOAD1
	phaseLOAD2
	phaseLOAD3 // load from persistence
	phaseSTEPPREPARE
	phaseSUPERSTEP
	phaseWRITE
	phaseRECOVER
	phaseFAILURE
)

const (
	START = iota
	WORKING
	FAILURE
	RECOVER
	SHUTDOWN
)

type RecoverableError struct {
	message string
}

func (e *RecoverableError) Error() string {
	return e.message
}

type RegistrationTimeoutError struct {
	message string
}

func (e *RegistrationTimeoutError) Error() string {
	return e.message
}
