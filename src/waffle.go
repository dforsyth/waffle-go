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
