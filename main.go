package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
)

// =========================
// Client
// =========================

type Client struct {
	username string
	inbox    chan string
	wg       *sync.WaitGroup
}

func NewClient(username string, wg *sync.WaitGroup) *Client {
	client := &Client{
		username: username,
		inbox:    make(chan string, 20),
		wg:       wg,
	}

	wg.Add(1)

	go client.run()

	return client
}

// Each client has its own goroutine.
func (c *Client) run() {
	defer c.wg.Done()

	for message := range c.inbox {
		outputMu.Lock()
		fmt.Printf("\n%s\n> ", message)
		outputMu.Unlock()
	}
}

// =========================
// Events
// =========================

type JoinEvent struct {
	username string
	client   *Client
	result   chan error
}

type MessageEvent struct {
	sender  string
	message string
}

type LeaveEvent struct {
	username string
	result   chan error
}

type ListUsersEvent struct {
	result chan []string
}

type ShutdownEvent struct{}

// =========================
// Server
// =========================

type Server struct {
	events chan interface{}

	users map[string]*Client

	mu sync.Mutex

	wg sync.WaitGroup

	done chan struct{}
}

func NewServer() *Server {
	return &Server{
		events: make(chan interface{}, 50),
		users:  make(map[string]*Client),
		done:   make(chan struct{}),
	}
}

// =========================
// Server Run
// =========================

func (s *Server) Run() {
	defer close(s.done)

	for {
		select {

		case event := <-s.events:

			switch e := event.(type) {

			case JoinEvent:
				s.handleJoin(e)

			case MessageEvent:
				s.handleMessage(e)

			case LeaveEvent:
				s.handleLeave(e)

			case ListUsersEvent:
				s.handleListUsers(e)

			case ShutdownEvent:
				s.handleShutdown()
				return
			}
		}
	}
}

// =========================
// Join
// =========================

func (s *Server) handleJoin(event JoinEvent) {

	s.mu.Lock()

	// Reject duplicate username
	if _, exists := s.users[event.username]; exists {
		s.mu.Unlock()

		event.result <- fmt.Errorf(
			"username %q already exists",
			event.username,
		)

		close(event.client.inbox)
		return
	}

	s.users[event.username] = event.client

	s.mu.Unlock()

	event.result <- nil

	// Notify everyone except the new user
	s.broadcastExcept(
		event.username,
		fmt.Sprintf(
			"User %s joined the chat.",
			event.username,
		),
	)
}

// =========================
// Message
// =========================

func (s *Server) handleMessage(event MessageEvent) {

	s.mu.Lock()

	_, exists := s.users[event.sender]

	s.mu.Unlock()

	if !exists {
		return
	}

	message := fmt.Sprintf(
		"[%s]: %s",
		event.sender,
		event.message,
	)

	// Sender does not receive own message
	s.broadcastExcept(event.sender, message)
}

// =========================
// Leave
// =========================

func (s *Server) handleLeave(event LeaveEvent) {

	s.mu.Lock()

	client, exists := s.users[event.username]

	if exists {
		delete(s.users, event.username)
	}

	s.mu.Unlock()

	if !exists {
		event.result <- fmt.Errorf(
			"user %q does not exist",
			event.username,
		)
		return
	}

	close(client.inbox)

	// Notify remaining users
	s.broadcastExcept(
		event.username,
		fmt.Sprintf(
			"User %s left the chat.",
			event.username,
		),
	)

	event.result <- nil
}

// =========================
// List Users
// =========================

func (s *Server) handleListUsers(event ListUsersEvent) {

	s.mu.Lock()

	users := make([]string, 0, len(s.users))

	for username := range s.users {
		users = append(users, username)
	}

	s.mu.Unlock()

	event.result <- users
}

// =========================
// Broadcast
// =========================

func (s *Server) broadcastExcept(
	excludedUser string,
	message string,
) {

	s.mu.Lock()
	defer s.mu.Unlock()

	for username, client := range s.users {

		if username == excludedUser {
			continue
		}

		client.inbox <- message
	}
}

// =========================
// Shutdown
// =========================

func (s *Server) handleShutdown() {

	s.mu.Lock()

	clients := make([]*Client, 0, len(s.users))

	for _, client := range s.users {
		clients = append(clients, client)
	}

	s.users = make(map[string]*Client)

	s.mu.Unlock()

	for _, client := range clients {
		close(client.inbox)
	}

	// Wait for all client goroutines
	s.wg.Wait()
}

// =========================
// Main
// =========================

var outputMu sync.Mutex

func main() {

	server := NewServer()

	// Start server goroutine
	go server.Run()

	// Handle Ctrl+C
	signalChan := make(chan os.Signal, 1)

	signal.Notify(
		signalChan,
		os.Interrupt,
		syscall.SIGTERM,
	)

	go func() {

		<-signalChan

		fmt.Println("\nShutting down server...")

		server.events <- ShutdownEvent{}

		<-server.done

		fmt.Println("All users disconnected.")
		fmt.Println("Goodbye!")

		os.Exit(0)
	}()

	printHelp()
	scanner := bufio.NewScanner(os.Stdin)

	var selectedUser string

	for {

		fmt.Print("> ")

		if !scanner.Scan() {
			break
		}

		input := strings.TrimSpace(scanner.Text())

		if input == "" {
			continue
		}

		// =========================
		// Create User
		// =========================

		if strings.HasPrefix(input, "/new ") {

			username := strings.TrimSpace(
				strings.TrimPrefix(input, "/new "),
			)

			if username == "" {
				fmt.Println("Username cannot be empty.")
				continue
			}

			if strings.ContainsAny(username, " \t") {
				fmt.Println("Username cannot contain spaces.")
				continue
			}

			client := NewClient(username, &server.wg)

			result := make(chan error)

			server.events <- JoinEvent{
				username: username,
				client:   client,
				result:   result,
			}

			err := <-result

			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			fmt.Printf(
				"User %q created successfully.\n",
				username,
			)

			selectedUser = username

			fmt.Printf(
				"Selected user: %s\n",
				selectedUser,
			)

			continue
		}

		// =========================
		// List Users
		// =========================

		if input == "/users" {

			result := make(chan []string)

			server.events <- ListUsersEvent{
				result: result,
			}

			users := <-result

			if len(users) == 0 {
				fmt.Println("No connected users.")
				continue
			}

			fmt.Println("Connected users:")

			for _, username := range users {

				if username == selectedUser {
					fmt.Printf(
						"  * %s (selected)\n",
						username,
					)
				} else {
					fmt.Printf(
						"  - %s\n",
						username,
					)
				}
			}

			continue
		}

		// =========================
		// Select User
		// =========================

		if strings.HasPrefix(input, "/use ") {

			username := strings.TrimSpace(
				strings.TrimPrefix(input, "/use "),
			)

			result := make(chan []string)

			server.events <- ListUsersEvent{
				result: result,
			}

			users := <-result

			found := false

			for _, user := range users {

				if user == username {
					found = true
					break
				}
			}

			if !found {
				fmt.Printf(
					"User %q does not exist.\n",
					username,
				)

				continue
			}

			selectedUser = username

			fmt.Printf(
				"Selected user: %s\n",
				selectedUser,
			)

			continue
		}

		// =========================
		// Send Message
		// =========================

		if strings.HasPrefix(input, "/send ") {

			if selectedUser == "" {
				fmt.Println(
					"No user selected. Use /use username first.",
				)

				continue
			}

			message := strings.TrimSpace(
				strings.TrimPrefix(input, "/send "),
			)

			if message == "" {
				fmt.Println("Message cannot be empty.")
				continue
			}

			server.events <- MessageEvent{
				sender:  selectedUser,
				message: message,
			}

			continue
		}

		// =========================
		// Remove User
		// =========================

		if strings.HasPrefix(input, "/remove ") {

			username := strings.TrimSpace(
				strings.TrimPrefix(input, "/remove "),
			)

			if username == "" {
				fmt.Println("Username cannot be empty.")
				continue
			}

			result := make(chan error)

			server.events <- LeaveEvent{
				username: username,
				result:   result,
			}

			err := <-result

			if err != nil {
				fmt.Printf("Error: %v\n", err)
				continue
			}

			fmt.Printf(
				"User %q removed successfully.\n",
				username,
			)

			if selectedUser == username {
				selectedUser = ""
				fmt.Println("No user is currently selected.")
			}

			continue
		}

		// =========================
		// Exit
		// =========================

		if input == "/exit" {

			fmt.Println("Shutting down server...")

			server.events <- ShutdownEvent{}

			<-server.done

			fmt.Println("All users disconnected.")
			fmt.Println("Goodbye!")

			return
		}

		// =========================
		// Help
		// =========================

		if input == "/help" {
			printHelp()
			continue
		}

		fmt.Println("Unknown command. Type /help.")
	}

	// Clean shutdown if input closes
	server.events <- ShutdownEvent{}
	<-server.done
}

// =========================
// Help
// =========================

func printHelp() {

	fmt.Println()
	fmt.Println("==========================================")
	fmt.Println("       Concurrent Chat System")
	fmt.Println("==========================================")
	fmt.Println()
	fmt.Println("Commands:")
	fmt.Println()
	fmt.Println("  /new username")
	fmt.Println("      Create a new user")
	fmt.Println()
	fmt.Println("  /users")
	fmt.Println("      List connected users")
	fmt.Println()
	fmt.Println("  /use username")
	fmt.Println("      Select a user")
	fmt.Println()
	fmt.Println("  /send message")
	fmt.Println("      Send a message as selected user")
	fmt.Println()
	fmt.Println("  /remove username")
	fmt.Println("      Remove a user")
	fmt.Println()
	fmt.Println("  /help")
	fmt.Println("      Show commands")
	fmt.Println()
	fmt.Println("  /exit")
	fmt.Println("      Exit the program")
	fmt.Println()
	fmt.Println("==========================================")
	fmt.Println()
}
