/**
 * Analytics Tracker Module
 * 
 * This module is completely isolated from game logic.
 * It handles event buffering, batching, and sending to the analytics endpoint.
 * 
 * Core principles:
 * - Does NOT control game behavior
 * - All events flow through trackEvent()
 * - Buffers events and flushes based on size/time/visibility
 */

class AnalyticsTracker {
    constructor() {
        // Generate session ID once per page load
        this.sessionId = this.generateUUID();
        
        // Web context - collected ONCE at session start, reused for all events
        // This enables session-level enrichment in Spark and reduces data duplication
        this.webContext = {
            timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
            language: navigator.language,
            // country will be added by backend from Cloud Run headers or IP
        };
        
        // Event buffer
        this.eventBuffer = [];
        this.maxBufferSize = 20;
        this.flushIntervalMs = 10000; // 10 seconds
        
        // Metrics for web-level tracking
        this.pageLoadTime = Date.now();
        this.gameStartTime = null;
        this.gameEndTime = null;
        this.lastActivityTime = Date.now();
        this.idleThresholdMs = 5000; // 5 seconds
        
        // Statistics
        this.totalEventsSent = 0;
        
        // Analytics endpoint
        this.endpoint = '/events';
        
        // Start periodic flush
        this.startPeriodicFlush();
        
        // Listen for page visibility changes and unload
        this.attachVisibilityListeners();
        
        // Track user activity for idle detection
        this.attachActivityListeners();
        
        console.log(`[Tracker] Initialized with session_id: ${this.sessionId}`);
        console.log(`[Tracker] Web context:`, this.webContext);
    }
    
    /**
     * Generate a UUID v4
     * @returns {string} UUID
     */
    generateUUID() {
        return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function(c) {
            const r = Math.random() * 16 | 0;
            const v = c === 'x' ? r : (r & 0x3 | 0x8);
            return v.toString(16);
        });
    }
    
    /**
     * Main tracking function - ALL events flow through here
     * @param {string} eventType - Type of event
     * @param {object} metadata - Event-specific metadata
     */
    trackEvent(eventType, metadata = {}) {
        const event = {
            event_id: this.generateUUID(),
            session_id: this.sessionId,
            event_type: eventType,
            timestamp: new Date().toISOString(),
            metadata: {
                ...metadata,
                // Add basic web context
                page_url: window.location.href,
                user_agent: navigator.userAgent,
            },
            // Web context - collected once at session start, same for all events
            web_context: this.webContext
        };
        
        // Add to buffer
        this.eventBuffer.push(event);
        
        console.log(`[Tracker] Event: ${eventType}`, metadata);
        
        // Update debug UI
        this.updateDebugUI();
        
        // Check if we should flush
        if (this.eventBuffer.length >= this.maxBufferSize) {
            this.flushEvents();
        }
    }
    
    /**
     * Track game start
     */
    trackGameStart() {
        this.gameStartTime = Date.now();
        this.trackEvent('game_start', {
            page_dwell_time_ms: Date.now() - this.pageLoadTime
        });
    }
    
    /**
     * Track word spawn
     * @param {string} word - The spawned word
     * @param {number} x - X position
     * @param {number} speed - Initial falling speed
     */
    trackWordSpawn(word, x, speed) {
        this.trackEvent('word_spawn', {
            word,
            spawn_x: x,
            initial_speed: speed
        });
    }
    
    /**
     * Track correct word typed
     * @param {string} word - The word typed
     * @param {number} timeToTypeMs - Time taken to type the word
     * @param {number} currentSpeed - Current game speed
     */
    trackWordTypedCorrect(word, timeToTypeMs, currentSpeed) {
        this.trackEvent('word_typed_correct', {
            word,
            time_to_type_ms: timeToTypeMs,
            current_speed: currentSpeed
        });
    }
    
    /**
     * Track incorrect typing attempt
     * @param {string} attempted - What the user typed
     * @param {string[]} availableWords - Words currently on screen
     */
    trackWordTypedIncorrect(attempted, availableWords) {
        this.trackEvent('word_typed_incorrect', {
            attempted,
            available_words: availableWords
        });
    }
    
    /**
     * Track word missed (reached bottom)
     * @param {string} word - The missed word
     * @param {number} speed - Speed when missed
     */
    trackWordMissed(word, speed) {
        this.trackEvent('word_missed', {
            word,
            speed
        });
    }
    
    /**
     * Track game over
     * @param {number} finalScore - Final score
     * @param {number} wordsTyped - Total words typed correctly
     * @param {number} wordsMissed - Total words missed
     * @param {number} finalSpeed - Final game speed
     */
    trackGameOver(finalScore, wordsTyped, wordsMissed, finalSpeed) {
        this.gameEndTime = Date.now();
        
        const activePlayTimeMs = this.gameEndTime - this.gameStartTime;
        const pageDwellTimeMs = this.gameEndTime - this.pageLoadTime;
        const idleTimeMs = this.calculateIdleTime();
        
        this.trackEvent('game_over', {
            final_score: finalScore,
            words_typed: wordsTyped,
            words_missed: wordsMissed,
            final_speed: finalSpeed,
            active_play_time_ms: activePlayTimeMs,
            page_dwell_time_ms: pageDwellTimeMs,
            idle_time_ms: idleTimeMs
        });
        
        // Flush immediately on game over
        this.flushEvents();
    }
    
    /**
     * Calculate total idle time
     * (This is simplified - in production you'd track this more precisely)
     * @returns {number} Estimated idle time in ms
     */
    calculateIdleTime() {
        // Simplified calculation: time since last activity if idle
        const timeSinceLastActivity = Date.now() - this.lastActivityTime;
        return timeSinceLastActivity > this.idleThresholdMs ? timeSinceLastActivity : 0;
    }
    
    /**
     * Flush buffered events to the server
     */
    async flushEvents() {
        if (this.eventBuffer.length === 0) {
            return;
        }
        
        const eventsToSend = [...this.eventBuffer];
        this.eventBuffer = [];
        
        console.log(`[Tracker] Flushing ${eventsToSend.length} events to ${this.endpoint}`);
        
        try {
            const response = await fetch(this.endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(eventsToSend)
            });
            
            if (response.ok) {
                this.totalEventsSent += eventsToSend.length;
                console.log(`[Tracker] ✓ Successfully sent ${eventsToSend.length} events`);
            } else {
                console.warn(`[Tracker] ⚠ Failed to send events: ${response.status} (backend not available)`);
            }
        } catch (error) {
            // Expected when running without backend - events are logged to console instead
            console.log(`[Tracker] ℹ Backend not available (${eventsToSend.length} events logged to console)`);
            // In production: retry logic, local storage backup, etc.
        }
        
        this.updateDebugUI();
    }
    
    /**
     * Start periodic flush timer
     */
    startPeriodicFlush() {
        setInterval(() => {
            if (this.eventBuffer.length > 0) {
                console.log('[Tracker] Periodic flush triggered');
                this.flushEvents();
            }
        }, this.flushIntervalMs);
    }
    
    /**
     * Attach visibility change listeners
     * Flush when page becomes hidden or before unload
     */
    attachVisibilityListeners() {
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                console.log('[Tracker] Page hidden, flushing events');
                this.flushEvents();
            }
        });
        
        window.addEventListener('beforeunload', () => {
            // Use sendBeacon for guaranteed delivery on page unload
            if (this.eventBuffer.length > 0) {
                console.log('[Tracker] Page unloading, sending beacon');
                const blob = new Blob([JSON.stringify(this.eventBuffer)], { type: 'application/json' });
                navigator.sendBeacon(this.endpoint, blob);
            }
        });
    }
    
    /**
     * Attach activity listeners for idle detection
     */
    attachActivityListeners() {
        const updateActivity = () => {
            this.lastActivityTime = Date.now();
        };
        
        document.addEventListener('keydown', updateActivity);
        document.addEventListener('mousemove', updateActivity);
        document.addEventListener('click', updateActivity);
    }
    
    /**
     * Update debug UI with current stats
     */
    updateDebugUI() {
        const sessionEl = document.getElementById('debugSession');
        const bufferEl = document.getElementById('debugBuffer');
        const sentEl = document.getElementById('debugSent');
        
        if (sessionEl) sessionEl.textContent = this.sessionId.substring(0, 8) + '...';
        if (bufferEl) bufferEl.textContent = this.eventBuffer.length;
        if (sentEl) sentEl.textContent = this.totalEventsSent;
    }
}

// Initialize global tracker instance
const tracker = new AnalyticsTracker();
