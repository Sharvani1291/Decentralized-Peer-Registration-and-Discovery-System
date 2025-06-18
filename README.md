# Decentralized Peer Registration and Discovery System

This project implements a decentralized peer registration and discovery mechanism using Java-based servers. It simulates a peer-to-peer (P2P) network where each peer can discover others through a bootstrap server and then communicate directly.

---

## 📦 Components

### 🧭 Bootstrap Server
- Acts as a central registry for all peers initially joining the network.
- Maintains a list of active peers.
- Responds to peer join/leave requests.

### 🔁 Normal Server (Peer)
- Registers with the bootstrap server.
- Retrieves a list of active peers.
- Capable of communicating directly with other peers for discovery or message passing.

---

## 🗂️ Files

| File                 | Description |
|----------------------|-------------|
| `BootstrapServer.java` | Manages peer registration and maintains active peer list |
| `NormalServer.java`     | Represents a peer that registers, discovers, and connects with others |

---

## 🚀 How to Run

1. **Compile all Java files**:
```bash
javac BootstrapServer.java NormalServer.java
```

2. **Run Bootstrap Server**:
```bash
java BootstrapServer
```

3. **Run Peer Servers** (in separate terminals):
```bash
java NormalServer
```

> ⚠️ Make sure each peer runs with a different port and optional name if coded to accept arguments.

---

## 🔄 Supported Operations

- Peer Registration
- Peer Discovery
- Peer Removal (on exit)
- Active peer listing

---

## 🧪 Example Scenario

1. Start the bootstrap server.
2. Launch 2 or more normal servers (peers).
3. Each peer registers and receives a list of others.
4. Peers can initiate direct communication or simulate message passing.

---

## 👩‍💻 Author

Sharvani Chelumalla  
M.S. in Computer Science – University of Georgia

---

## 📜 License

This project is for academic use and demonstration purposes only.