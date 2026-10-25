package iotedge

import "runtime/debug"

// Set at build time with -ldflags "-X github.com/pat-rohn/go-iotedge.<name>=..."
// (GoReleaser does this for releases).
var (
	buildVersion = ""
	buildDate    = "unknown"
	commitHash   = "unknown"
)

type VersionInfo struct {
	// Version is the release tag, empty for builds outside a release.
	Version string `json:"version,omitempty"`
	Date    string `json:"date"`
	Commit  string `json:"commit"`
}

func CurrentVersion() VersionInfo {
	version := VersionInfo{
		Version: buildVersion,
		Date:    buildDate,
		Commit:  commitHash,
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
