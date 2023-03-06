package machinery

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
	"unsafe"
)

type Server struct {
	Port    int
	DBPath  string
	LogPath string
	*exec.Cmd

	Stdout *bytes.Buffer
	Stderr *bytes.Buffer
}

func NewServer(port int, dbpath string, logpath string) *Server {
	return &Server{
		port,
		dbpath,
		logpath,
		exec.Command("mongod", "--dbpath", dbpath, "--port", fmt.Sprintf("%d", port), "--logpath", logpath),
		&bytes.Buffer{},
		&bytes.Buffer{},
	}
}

func (server *Server) Start() error {
	server.Cmd.Stdout = server.Stdout
	server.Cmd.Stderr = server.Stderr

	return server.Cmd.Start()
}

func (server *Server) StartAndWaitForListening(timeout time.Duration) error {
	if err := server.Start(); err != nil {
		return err
	}

	return server.WaitForListening(timeout)
}

func (server *Server) WaitForListening(timeout time.Duration) error {
	startTime := time.Now()
	for time.Since(startTime) < timeout {
		shell := exec.Command("mongo", "--port", fmt.Sprintf("%d", server.Port), "--quiet")
		stdin, err := shell.StdinPipe()
		if err != nil {
			panic(err)
		}

		if err := shell.Start(); err != nil {
			panic(err)
		}

		stdin.Close()

		if err := shell.Wait(); err != nil {
			continue
		} else {
			return nil
		}
	}
	panic("timeout")
}

func (server *Server) GetStdout() string {
	return server.Stdout.String()
}

func (server *Server) GetStderr() string {
	return server.Stderr.String()
}

func (server *Server) SigInt() {
	server.Process.Signal(os.Interrupt)
}

func (server *Server) WaitAndPrint() {
	err := server.Wait()
	if err != nil {
		panic(err)
	}

	fmt.Println(server.Stdout.String())
	fmt.Println(server.Stderr.String())
}

func (server *Server) Execute(db string, cmd string) string {
	shell := exec.Command("mongo", "--port", fmt.Sprintf("%d", server.Port), "--quiet", db)
	stdin, err := shell.StdinPipe()
	if err != nil {
		panic(err)
	}

	stdout := &bytes.Buffer{}
	shell.Stdout = stdout
	if err := shell.Start(); err != nil {
		panic(err)
	}

	stdout.Write([]byte(db))
	stdout.Write([]byte("> "))
	stdout.Write([]byte(cmd))
	stdout.Write([]byte("\n"))
	stdin.Write([]byte(cmd))
	stdin.Write([]byte("\n"))
	stdin.Close()

	if err := shell.Wait(); err != nil {
		panic(err)
	}
	return stdout.String()
}

func (server *Server) SpawnHttp(port int) error {
	return nil
}

func (server *Server) SpawnShell() error {
	return SpawnShell(server.Port)
}

func SpawnShell(port int) error {
	shell := exec.Command("mongo", "--port", fmt.Sprintf("%d", port))
	stdin, err := shell.StdinPipe()
	if err != nil {
		panic(err)
	}

	stdout, err := shell.StdoutPipe()
	if err != nil {
		panic(err)
	}

	go func(shell *exec.Cmd, input io.WriteCloser, output io.ReadCloser) {
		state, err := MakeRaw(0)
		if err != nil {
			panic(err)
		}
		defer restoreTerm(0, state)

		buf := make([]byte, 20000)
		for {
			read, err := os.Stdin.Read(buf)
			fmt.Printf("Read: %d Err: %v\n", read, err)
			fmt.Println("\t", buf[:read])
			if err == io.EOF {
				break
			}
			if err != nil {
				panic(err)
			}

			var wrote int = 0
			for wrote < read {
				nWrote, err := input.Write(buf[wrote:read])
				if err != nil {
					panic(err)
				}

				wrote += nWrote
			}
		}

		input.Close()
	}(shell, stdin, stdout)

	shell.Stdout = os.Stdout
	shell.Stderr = os.Stdout
	if err := shell.Start(); err != nil {
		panic(err)
	}

	if err := shell.Wait(); err != nil {
		panic(err)
	}

	return nil
}

// These constants are declared here, rather than importing
// them from the syscall package as some syscall packages, even
// on linux, for example gccgo, do not declare them.
const ioctlReadTermios = 0x5401  // syscall.TCGETS
const ioctlWriteTermios = 0x5402 // syscall.TCSETS

type Termios syscall.Termios

// State contains the state of a terminal.
type State struct {
	termios Termios
}

func MakeRaw(fd int) (*State, error) {
	var oldState State

	if termios, err := getTermios(fd); err != nil {
		return nil, err
	} else {
		oldState.termios = *termios
	}

	newState := oldState.termios
	// This attempts to replicate the behaviour documented for cfmakeraw in
	// the termios(3) manpage.
	newState.Iflag &^= syscall.IGNBRK | syscall.BRKINT | syscall.PARMRK | syscall.ISTRIP | syscall.INLCR | syscall.IGNCR | syscall.ICRNL | syscall.IXON
	// newState.Oflag &^= syscall.OPOST
	newState.Lflag &^= syscall.ECHO | syscall.ECHONL | syscall.ICANON | syscall.ISIG | syscall.IEXTEN
	newState.Cflag &^= syscall.CSIZE | syscall.PARENB
	newState.Cflag |= syscall.CS8

	newState.Cc[syscall.VMIN] = 1
	newState.Cc[syscall.VTIME] = 0

	return &oldState, setTermios(fd, &newState)
}

func getTermios(fd int) (*Termios, error) {
	termios := new(Termios)
	_, _, err := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), ioctlReadTermios, uintptr(unsafe.Pointer(termios)), 0, 0, 0)
	if err != 0 {
		return nil, err
	}
	return termios, nil
}

func setTermios(fd int, termios *Termios) error {
	_, _, err := syscall.Syscall6(syscall.SYS_IOCTL, uintptr(fd), ioctlWriteTermios, uintptr(unsafe.Pointer(termios)), 0, 0, 0)
	if err != 0 {
		return err
	}
	return nil
}

// Restore restores the terminal connected to the given file descriptor to a
// previous state.
func restoreTerm(fd int, state *State) error {
	return setTermios(fd, &state.termios)
}
