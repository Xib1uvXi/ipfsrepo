package lsblk

import (
	"fmt"
	"github.com/Xib1uvXi/ipfsrepo/pkg/linuxutils/cmd"
	"github.com/goccy/go-json"
)

const (
	// CmdTmpl adds device name, if add empty string - command will print info about all devices
	CmdTmpl = "lsblk %s --paths --json --bytes --fs " +
		"--output NAME,TYPE,SIZE,ROTA,MOUNTPOINT,FSTYPE,PARTUUID,LABEL,UUID"

	// outputKey is the key to find block devices in lsblk json output
	outputKey = "blockdevices"
)

// BlockDevice is the struct that represents output of lsblk command for a device
type BlockDevice struct {
	Name       string         `json:"name,omitempty"`
	Type       string         `json:"type,omitempty"`
	Size       uint64         `json:"size,omitempty"`
	Rota       bool           `json:"rota,omitempty"`
	MountPoint string         `json:"mountpoint,omitempty"`
	FSType     string         `json:"fstype,omitempty"`
	PartUUID   string         `json:"partuuid,omitempty"`
	Label      string         `json:"label,omitempty"`
	UUID       string         `json:"uuid,omitempty"`
	Children   []*BlockDevice `json:"children,omitempty"`
}

type Cmd struct {
	*cmd.Executor
}

func NewCmd() *Cmd {
	return &Cmd{Executor: cmd.NewExecutor()}
}

// GetBlockDevices run os lsblk command for device and construct BlockDevice struct based on output
// Receives device path. If device is empty string, info about all devices will be collected
// Returns slice of BlockDevice structs or error if something went wrong
func (c *Cmd) GetBlockDevices(device string) ([]BlockDevice, error) {
	cmdStr := fmt.Sprintf(CmdTmpl, device)
	stdOut, _, err := c.ExecuteCmd(cmdStr)
	if err != nil {
		return nil, err
	}

	rawOut := make(map[string][]BlockDevice, 1)
	if err := json.Unmarshal([]byte(stdOut), &rawOut); err != nil {
		return nil, fmt.Errorf("unable to unmarshal output to BlockDevice instance, error: %v", err)
	}

	var (
		devs []BlockDevice
		ok   bool
	)
	if devs, ok = rawOut[outputKey]; !ok {
		return nil, fmt.Errorf("unexpected lsblk output format, missing \"%s\" key", outputKey)
	}

	return devs, nil
}

// SearchBlockDevice searches for a block device by fstype, lable, mountpoint
func (c *Cmd) SearchBlockDevice(fstype, label string, hasMountPoint bool) ([]BlockDevice, error) {
	devices, err := c.GetBlockDevices("")
	if err != nil {
		return nil, err
	}

	var res []BlockDevice = make([]BlockDevice, 0)
	for _, dev := range devices {
		if dev.FSType == fstype && dev.Label == label {
			if hasMountPoint && dev.MountPoint != "" {
				res = append(res, dev)
			} else if !hasMountPoint {
				res = append(res, dev)
			}
		}
	}

	return res, nil
}
