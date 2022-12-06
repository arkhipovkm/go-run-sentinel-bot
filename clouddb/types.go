package clouddb

type VersionsInfo struct {
	Camera         string
	Sensogram      string
	FSPRefdesignHW string `json:"fsp_refdesign_hw"`
	FSPRefdesignFW string `json:"fsp_refdesign_fw"`
	FSPControlHW   string `json:"fsp_control_hw"`
	FSPControlFW   string `json:"fsp_control_fw"`
}

type DeviceInfo struct {
	DeviceClass        string
	PumpPower          float64
	SensogramFrequency float64 // TODO: int ???
	ShellSerial        string
	CoreSensorSerial   string
	CameraExposure     float64
	CameraFrequency    float64
	CameraWidth        float64
	CameraHeight       float64
	CameraDepth        float64
	VCSELPower         float64 `json:"vcselPower"`
	SpotsGrid          []int
	PeakPhaseGains     []float64
	PeakPhaseOffsets   []float64
	PeakMaskBottomLeft []float64
	PeakMaskTopRight   []float64
	PeakMaskAngle      []float64
	PeakMaskRadius     []float64
	Versions           *VersionsInfo
}

type DeviceMetadata struct {
	ID             string
	RunUID         string
	HostName       string
	TimestampStart int
	Info           *DeviceInfo
}

type RunMetadata struct {
	UID    string
	ID     string
	Device *DeviceMetadata

	Name           string
	ProtocolName   string
	RecordsCount   int
	TimestampStart int
	TimestampEnd   int
	Tags           []string
	Cycles         []int
	ItemNames      []string
	IsFinished     bool
	SWVersion      string

	value int
}

type ItemMetadata struct {
	RunUID         string
	RunID          string
	DeviceID       string
	Name           string
	Description    string
	Index          int
	Cycle          int
	TimestampStart int // unix milliseconds
	TimestampEnd   int // unix milliseconds
	UserTags       []string
	value          int
}

type SensogramFrame struct {
	Timestamp                   int
	Section                     string
	Data                        []float64
	Temperature                 *float64
	Humidity                    *float64
	ThermodesorptionTemperature *float64

	// SDOK fields
	SunriseCo2    *float64
	PidVoc        *float64
	ZephyrAirflow *float64
	Pms1          *float64
	Pms25         *float64
	Pms10         *float64
	BmeVoc        *float64
	BmeCo2        *float64

	ItemIndex int
	Cycle     int
	Spots     []string
}

type FrameBase struct {
	Timestamp int
	Value     float64
	Section   string
}

type Record struct {
	Run    *RunMetadata
	Device *DeviceMetadata
	Item   *ItemMetadata
	Spots  *[]string
	Frames []*SensogramFrame
}

type InfluxDBConfig struct {
	Name   string
	URL    string
	Token  string
	Org    string
	Bucket string
}

type HandlesConfig struct {
	HandleIDs []string
	ItemNames []string
	NRecords  int
}

type ChunkedResult struct {
	N        int
	HashCode string
	Frames   []*SensogramFrame
	Err      error
}
