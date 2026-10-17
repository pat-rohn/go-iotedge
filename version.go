package iotedge

import "runtime/debug"

var (
	buildDate  = "unknown"
	commitHash = "unknown"
)

type VersionInfo struct {
	Date   string `json:"date"`
	Commit string `json:"commit"`
}

func CurrentVersion() VersionInfo {
	version := VersionInfo{
		Date:   buildDate,
		Commit: commitHash,
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return version
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.time":
			if version.Date == "unknown" {
				version.Date = setting.Value
			}
		case "vcs.revision":
			if version.Commit == "unknown" {
				version.Commit = setting.Value
			}
		}
	}
	return version
}
