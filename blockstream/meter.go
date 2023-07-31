package blockstream

type Meter interface {
	AddBytesRead(int)
	AddBytesWritten(int)
}

type noopMeter struct{}

func (_ *noopMeter) AddBytesWritten(n int) { return }
func (_ *noopMeter) AddBytesRead(n int)    { return }
