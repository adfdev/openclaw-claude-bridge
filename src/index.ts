import http from 'http';
import { app, statusApp, stats, saveState } from './server';

const API_PORT = parseInt(process.env.OPENCLAW_BRIDGE_PORT || '3456');
const STATUS_PORT = parseInt(process.env.OPENCLAW_BRIDGE_STATUS_PORT || '3458');

// API server — localhost only (OpenClaw access)
const apiServer = http.createServer(app).listen(API_PORT, '127.0.0.1', () => {
    console.log(`[openclaw-claude-bridge] API     → http://127.0.0.1:${API_PORT}`);
});

// Status server — all interfaces (LAN access for dashboard)
const statusServer = http.createServer(statusApp).listen(STATUS_PORT, '0.0.0.0', () => {
    console.log(`[openclaw-claude-bridge] Status  → http://0.0.0.0:${STATUS_PORT}`);
});

// --- Graceful shutdown ---
let shuttingDown = false;

function gracefulShutdown(signal: string): void {
    if (shuttingDown) return;
    shuttingDown = true;
    console.log(`[openclaw-claude-bridge] ${signal} received — stopping new connections, waiting for active requests...`);

    saveState();

    apiServer.close();
    statusServer.close();

    const check = setInterval(() => {
        const active = stats.activeRequests;
        if (active === 0) {
            clearInterval(check);
            console.log(`[openclaw-claude-bridge] All requests completed — shutting down cleanly`);
            process.exit(0);
        } else {
            console.log(`[openclaw-claude-bridge] Waiting for ${active} active request(s)...`);
        }
    }, 5000);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));
