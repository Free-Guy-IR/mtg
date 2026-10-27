package main_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

var digestPinnedImage = regexp.MustCompile(`^[^@\s]+@sha256:[0-9a-f]{64}$`)

func dockerInstructions(content string) [][]string {
	var (
		instructions [][]string
		pending      []string
	)

	for line := range strings.Lines(content) {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}

		continued := strings.HasSuffix(trimmed, "\\")
		pending = append(pending, strings.Fields(strings.TrimSuffix(trimmed, "\\"))...)

		if !continued && len(pending) > 0 {
			instructions = append(instructions, pending)
			pending = nil
		}
	}

	if len(pending) > 0 {
		instructions = append(instructions, pending)
	}

	return instructions
}

type flagArg struct {
	name  string
	value string
}

type imageRef struct {
	ref      string
	fromFlag bool
}

func splitFlags(args []string) ([]flagArg, []string) {
	var flags []flagArg

	index := 0
	for index < len(args) {
		name, ok := strings.CutPrefix(args[index], "--")
		if !ok {
			break
		}

		if n, v, found := strings.Cut(name, "="); found {
			flags = append(flags, flagArg{n, v})
			index++

			continue
		}

		value := ""
		if index+1 < len(args) && !strings.HasPrefix(args[index+1], "--") {
			value = args[index+1]
			index++
		}

		flags = append(flags, flagArg{name, value})
		index++
	}

	return flags, args[index:]
}

func mountImages(spec string) []string {
	var refs []string

	for option := range strings.SplitSeq(spec, ",") {
		if value, ok := strings.CutPrefix(option, "from="); ok {
			refs = append(refs, value)
		}
	}

	return refs
}

func imageReferences(instruction []string) []imageRef {
	flags, rest := splitFlags(instruction[1:])

	var refs []imageRef

	switch strings.ToUpper(instruction[0]) {
	case "FROM":
		if len(rest) == 0 {
			return []imageRef{{"", false}}
		}

		refs = append(refs, imageRef{rest[0], false})
	case "COPY":
		for _, flag := range flags {
			if strings.EqualFold(flag.name, "from") {
				refs = append(refs, imageRef{flag.value, true})
			}
		}
	case "RUN":
		for _, flag := range flags {
			if !strings.EqualFold(flag.name, "mount") {
				continue
			}

			for _, ref := range mountImages(flag.value) {
				refs = append(refs, imageRef{ref, true})
			}
		}
	}

	return refs
}

func stageName(instruction []string) (string, bool) {
	if !strings.EqualFold(instruction[0], "FROM") {
		return "", false
	}

	_, rest := splitFlags(instruction[1:])
	if len(rest) == 3 && strings.EqualFold(rest[1], "AS") {
		return strings.ToLower(rest[2]), true
	}

	return "", false
}

func isStageIndex(ref string) bool {
	if ref == "" {
		return false
	}

	for _, r := range ref {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

func isEarlierStageIndex(ref string, stageCount int) bool {
	if !isStageIndex(ref) {
		return false
	}

	index, err := strconv.Atoi(ref)

	return err == nil && index >= 0 && index < stageCount
}

func unpinnedDockerImages(content string) []string {
	var unpinned []string

	stages := map[string]bool{}
	stageCount := 0

	for _, instruction := range dockerInstructions(content) {
		if strings.EqualFold(instruction[0], "FROM") {
			stageCount++
		}

		for _, image := range imageReferences(instruction) {
			switch {
			case strings.EqualFold(image.ref, "scratch"),
				stages[strings.ToLower(image.ref)],
				image.fromFlag && isEarlierStageIndex(image.ref, stageCount),
				digestPinnedImage.MatchString(image.ref):
			default:
				unpinned = append(unpinned, image.ref)
			}
		}

		if name, ok := stageName(instruction); ok {
			stages[name] = true
		}
	}

	return unpinned
}

func isDockerfile(name string) bool {
	lower := strings.ToLower(name)

	return lower == "dockerfile" ||
		lower == "containerfile" ||
		strings.HasPrefix(lower, "dockerfile.") ||
		strings.HasSuffix(lower, ".dockerfile")
}

func TestDockerfilesPinImagesByDigest(t *testing.T) {
	var dockerfiles []string

	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() && entry.Name() == ".git" {
			return filepath.SkipDir
		}

		if !entry.IsDir() && isDockerfile(entry.Name()) {
			dockerfiles = append(dockerfiles, path)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if len(dockerfiles) == 0 {
		t.Fatal("no Dockerfile found")
	}

	for _, path := range dockerfiles {
		content, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}

		if unpinned := unpinnedDockerImages(string(content)); len(unpinned) > 0 {
			t.Errorf("%s uses images that are not pinned by sha256 digest: %q", path, unpinned)
		}
	}
}

func TestUnpinnedDockerImages(t *testing.T) {
	pin := "@sha256:" + strings.Repeat("0a", 32)

	cases := map[string]struct {
		content string
		want    []string
	}{
		"floating tag": {
			content: "FROM golang:1.26-alpine AS build\nFROM scratch\nCOPY --from=build /app/mtg /mtg\n",
			want:    []string{"golang:1.26-alpine"},
		},
		"tag and digest": {
			content: "FROM golang:1.26.10-alpine" + pin + " AS build\nFROM scratch\nCOPY --from=build /app/mtg /mtg\n",
		},
		"digest only": {
			content: "FROM golang" + pin + "\n",
		},
		"no tag": {
			content: "FROM ubuntu\n",
			want:    []string{"ubuntu"},
		},
		"short digest": {
			content: "FROM alpine@sha256:abc\n",
			want:    []string{"alpine@sha256:abc"},
		},
		"uppercase digest": {
			content: "FROM alpine@sha256:" + strings.Repeat("0A", 32) + "\n",
			want:    []string{"alpine@sha256:" + strings.Repeat("0A", 32)},
		},
		"platform flag and lower case stage": {
			content: "from --platform=$BUILDPLATFORM alpine:3.22 as base\nFROM base\n",
			want:    []string{"alpine:3.22"},
		},
		"stage used before it is declared": {
			content: "FROM base\nFROM alpine" + pin + " AS base\n",
			want:    []string{"base"},
		},
		"copy from image": {
			content: "FROM golang" + pin + "\nCOPY --from=nginx:1.29 /etc/nginx /etc/nginx\n",
			want:    []string{"nginx:1.29"},
		},
		"copy from stage index": {
			content: "FROM golang" + pin + "\nFROM scratch\nCOPY --from=0 /app /app\n",
		},
		"run mount from image": {
			content: "FROM golang" + pin + "\nRUN --mount=type=bind,from=busybox:1.37,target=/bb \\\n    ls /bb\n",
			want:    []string{"busybox:1.37"},
		},
		"run mount from stage and cache": {
			content: "FROM golang" + pin + " AS tools\nFROM scratch\n" +
				"RUN --mount=type=cache,target=/var/cache/apk --mount=type=bind,from=tools,target=/t \\\n    true\n",
		},
		"commented out instruction": {
			content: "# FROM ubuntu\nFROM scratch\n",
		},
		"continued instruction": {
			content: "FROM \\\n    debian:13\n",
			want:    []string{"debian:13"},
		},
		"missing image": {
			content: "FROM\n",
			want:    []string{""},
		},
		"from numeric base image is not a stage index": {
			content: "FROM 123\n",
			want:    []string{"123"},
		},
		"from zero base image is not a stage index": {
			content: "FROM golang" + pin + "\nFROM 0\n",
			want:    []string{"0"},
		},
		"copy from numeric index after a named stage": {
			content: "FROM golang" + pin + " AS build\nFROM scratch\nCOPY --from=0 /app /app\n",
		},
		"run mount space form from image": {
			content: "FROM golang" + pin + "\nRUN --mount type=bind,from=busybox:1.37,target=/bb \\\n    ls /bb\n",
			want:    []string{"busybox:1.37"},
		},
		"run mount space form pinned": {
			content: "FROM golang" + pin + "\nRUN --mount type=bind,from=busybox" + pin + ",target=/bb \\\n    ls /bb\n",
		},
		"copy boolean flag before from image": {
			content: "FROM golang" + pin + "\nCOPY --link --from=nginx:1.29 /a /b\n",
			want:    []string{"nginx:1.29"},
		},
		"copy from out of range numeric index is an image": {
			content: "FROM golang" + pin + "\nCOPY --from=99 /a /b\n",
			want:    []string{"99"},
		},
		"run mount from out of range numeric index is an image": {
			content: "FROM golang" + pin + "\nRUN --mount=type=bind,from=5,target=/x \\\n    true\n",
			want:    []string{"5"},
		},
		"copy from zero index with a single stage": {
			content: "FROM golang" + pin + "\nCOPY --from=0 /a /b\n",
		},
		"copy from index one with two stages": {
			content: "FROM golang" + pin + " AS a\nFROM scratch\nCOPY --from=1 /a /b\n",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			if got := unpinnedDockerImages(testCase.content); !slices.Equal(got, testCase.want) {
				t.Errorf("unpinnedDockerImages() = %q, want %q", got, testCase.want)
			}
		})
	}
}
