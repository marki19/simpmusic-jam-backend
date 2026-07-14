const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Store active jam sessions
// Map<roomId, { hostId, hostWs, guests: Map<guestId, ws>, permissions: Object, state: Object }>
const sessions = new Map();

// Helper to generate 6-character room codes
function generateRoomCode() {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let code;
    do {
        code = '';
        for (let i = 0; i < 6; i++) {
            code += chars.charAt(Math.floor(Math.random() * chars.length));
        }
    } while (sessions.has(code));
    return code;
}

// Broadcast message to everyone in the room
function broadcast(roomId, message, excludeWs = null) {
    const session = sessions.get(roomId);
    if (!session) return;
    
    const data = JSON.stringify(message);
    
    if (session.hostWs !== excludeWs && session.hostWs.readyState === WebSocket.OPEN) {
        session.hostWs.send(data);
    }
    
    for (const [guestId, guestWs] of session.guests.entries()) {
        if (guestWs !== excludeWs && guestWs.readyState === WebSocket.OPEN) {
            guestWs.send(data);
        }
    }
}

wss.on('connection', (ws) => {
    let currentRoomId = null;
    let currentUserId = null;

    ws.on('message', (messageRaw) => {
        try {
            const msg = JSON.parse(messageRaw.toString());
            
            switch (msg.type) {
                case 'CREATE_SESSION': {
                    currentUserId = msg.userId;
                    currentRoomId = generateRoomCode();
                    
                    sessions.set(currentRoomId, {
                        hostId: currentUserId,
                        hostWs: ws,
                        guests: new Map(),
                        permissions: {
                            canControlPlayback: true,
                            canAddQueue: true
                        },
                        state: {
                            currentSong: null,
                            playbackPosition: 0,
                            isPlaying: false,
                            queue: []
                        }
                    });
                    
                    ws.send(JSON.stringify({ 
                        type: 'SESSION_CREATED', 
                        roomId: currentRoomId 
                    }));
                    break;
                }
                
                case 'JOIN_SESSION': {
                    const { roomId, userId } = msg;
                    if (!sessions.has(roomId)) {
                        ws.send(JSON.stringify({ type: 'ERROR', message: 'Room not found' }));
                        return;
                    }
                    
                    currentRoomId = roomId;
                    currentUserId = userId;
                    
                    const session = sessions.get(roomId);
                    session.guests.set(userId, ws);
                    
                    // Send current state to the new guest
                    ws.send(JSON.stringify({
                        type: 'SESSION_JOINED',
                        roomId: currentRoomId,
                        hostId: session.hostId, // Explicitly pass hostId
                        state: session.state,
                        permissions: session.permissions
                    }));
                    
                    // Notify others
                    broadcast(currentRoomId, { 
                        type: 'GUEST_JOINED', 
                        userId 
                    }, ws);
                    break;
                }
                
                case 'UPDATE_PERMISSIONS': {
                    if (!currentRoomId) return;
                    const session = sessions.get(currentRoomId);
                    if (session.hostId !== currentUserId) return;
                    
                    session.permissions = { ...session.permissions, ...msg.permissions };
                    broadcast(currentRoomId, {
                        type: 'PERMISSIONS_UPDATED',
                        permissions: session.permissions
                    });
                    break;
                }
                
                case 'SYNC_STATE': {
                    // Host periodically syncs the master state
                    if (!currentRoomId) return;
                    const session = sessions.get(currentRoomId);
                    // Also allow sync if the guest was just promoted
                    if (session.hostId !== currentUserId) return;
                    
                    session.state = { ...session.state, ...msg.state };
                    broadcast(currentRoomId, {
                        type: 'STATE_SYNC',
                        state: session.state
                    }, ws); // Send to all guests
                    break;
                }
                
                case 'COMMAND': {
                    if (!currentRoomId) return;
                    const session = sessions.get(currentRoomId);
                    const isHost = session.hostId === currentUserId;
                    
                    // Check permissions if guest
                    if (!isHost) {
                        if (['PLAY', 'PAUSE', 'SEEK', 'SKIP'].includes(msg.command) && !session.permissions.canControlPlayback) {
                            return; // Not allowed
                        }
                        if (msg.command === 'ADD_TO_QUEUE' && !session.permissions.canAddQueue) {
                            return; // Not allowed
                        }
                    }
                    
                    // Relay the command to everyone else
                    broadcast(currentRoomId, {
                        type: 'COMMAND',
                        command: msg.command,
                        payload: msg.payload,
                        userId: currentUserId
                    }, ws);
                    break;
                }
            }
        } catch (e) {
            console.error('Invalid message format', e);
        }
    });

    ws.on('close', () => {
        if (!currentRoomId) return;
        const session = sessions.get(currentRoomId);
        if (!session) return;
        
        const isHost = session.hostId === currentUserId;
        
        if (isHost) {
            if (session.guests.size > 0) {
                // Host migration: Pick the oldest guest
                const newHostId = Array.from(session.guests.keys())[0];
                const newHostWs = session.guests.get(newHostId);
                
                session.hostId = newHostId;
                session.hostWs = newHostWs;
                session.guests.delete(newHostId);
                
                broadcast(currentRoomId, { type: 'HOST_LEFT', userId: currentUserId });
            } else {
                // No guests left, end session
                broadcast(currentRoomId, { type: 'SESSION_ENDED' });
                sessions.delete(currentRoomId);
            }
        } else {
            // Guest left
            session.guests.delete(currentUserId);
            broadcast(currentRoomId, { type: 'GUEST_LEFT', userId: currentUserId });
        }
    });
});

app.get('/health', (req, res) => res.json({ status: 'ok', sessions: sessions.size }));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`JAM WebSocket Server running on port ${PORT}`);
});
