/**
 * Acid Rain Typing Game
 * 
 * Game logic is completely separate from tracking logic.
 * The game calls tracker methods to record events, but the tracker
 * does not influence game behavior.
 */

class AcidRainGame {
    constructor() {
        // Canvas setup
        this.canvas = document.getElementById('gameCanvas');
        this.ctx = this.canvas.getContext('2d');
        
        // Language setting
        this.currentLanguage = 'en'; // 'en' or 'ko'
        
        // Game state
        this.state = 'idle'; // idle, running, game_over
        this.score = 0;
        this.wordsTyped = 0;
        this.wordsMissed = 0;
        this.currentSpeed = 1.0;
        this.speedIncrement = 0.1;
        this.speedIncreaseInterval = 5000; // Increase speed every 5 seconds
        
        // Timer
        this.gameDurationMs = 90000; // 90 seconds
        this.gameStartTime = 0;
        this.remainingTimeMs = this.gameDurationMs;
        
        // Word list management (no duplicates)
        this.availableWords = [];
        this.currentWordIndex = 0;
        
        // Falling words
        this.fallingWords = [];
        this.baseSpawnInterval = 2000; // Starting spawn interval (2 seconds)
        this.currentSpawnInterval = this.baseSpawnInterval;
        this.minSpawnInterval = 800; // Fastest spawn rate (0.8 seconds)
        this.lastSpawnTime = 0;
        this.baseSpeed = 0.5; // Base falling speed (pixels per frame)
        
        // User input
        this.currentInput = '';
        
        // Timing for tracking
        this.wordStartTime = null; // When user starts typing current word
        
        // Animation
        this.animationId = null;
        this.lastFrameTime = 0;
        this.lastSpeedIncreaseTime = 0;
        
        // UI elements
        this.scoreDisplay = document.getElementById('scoreDisplay');
        this.timerDisplay = document.getElementById('timerDisplay');
        this.speedDisplay = document.getElementById('speedDisplay');
        this.missedDisplay = document.getElementById('missedDisplay');
        this.inputDisplay = document.getElementById('inputDisplay');
        
        // Create hidden input field for IME support (Korean, Japanese, Chinese)
        this.hiddenInput = document.createElement('input');
        this.hiddenInput.type = 'text';
        this.hiddenInput.style.position = 'absolute';
        this.hiddenInput.style.left = '-9999px';
        this.hiddenInput.style.opacity = '0';
        document.body.appendChild(this.hiddenInput);
        
        // Screens
        this.startScreen = document.getElementById('startScreen');
        this.gameOverScreen = document.getElementById('gameOverScreen');
        
        // Bind events
        this.bindEvents();
        
        console.log('[Game] Initialized');
    }
    
    /**
     * Bind UI events
     */
    bindEvents() {
        // Language selection on start screen
        document.getElementById('langEnButton').addEventListener('click', () => {
            this.startGame('en');
        });
        
        document.getElementById('langKoButton').addEventListener('click', () => {
            this.startGame('ko');
        });
        
        // Language selection on game over screen
        document.getElementById('restartEnButton').addEventListener('click', () => {
            this.restartGame('en');
        });
        
        document.getElementById('restartKoButton').addEventListener('click', () => {
            this.restartGame('ko');
        });
        
        // Canvas click to focus
        this.canvas.addEventListener('click', () => {
            if (this.state === 'running') {
                this.hiddenInput.focus();
            }
        });
        
        // Monitor hidden input for proper IME handling
        this.hiddenInput.addEventListener('input', (e) => {
            if (this.state !== 'running') return;
            
            const newValue = e.target.value;
            
            // Track word start time when first character is entered
            if (newValue.length === 1 && this.currentInput.length === 0) {
                this.wordStartTime = Date.now();
            }
            
            this.currentInput = newValue;
            this.updateInputDisplay();
            
            console.log(`[Game] Input: "${this.currentInput}"`);
        });
        
        // Handle keyboard shortcuts
        this.hiddenInput.addEventListener('keydown', (e) => {
            if (this.state !== 'running') return;
            
            if (e.key === 'Enter') {
                e.preventDefault();
                this.handleSubmit();
            } else if (e.key === 'Backspace' && this.hiddenInput.value === '') {
                // Backspace on empty input
                e.preventDefault();
            }
        });
        
        // Keep hidden input focused during game
        document.addEventListener('click', () => {
            if (this.state === 'running') {
                this.hiddenInput.focus();
            }
        });
    }
    
    /**
     * Start the game
     * @param {string} language - 'en' or 'ko'
     */
    startGame(language = 'en') {
        console.log(`[Game] Starting game with language: ${language}`);
        
        // Set language
        this.currentLanguage = language;
        
        // Reset game state
        this.state = 'running';
        this.score = 0;
        this.wordsTyped = 0;
        this.wordsMissed = 0;
        this.currentSpeed = 1.0;
        this.fallingWords = [];
        this.currentInput = '';
        this.lastSpawnTime = 0;
        this.lastSpeedIncreaseTime = 0;
        this.currentSpawnInterval = this.baseSpawnInterval;
        
        // Initialize timer
        this.gameStartTime = Date.now();
        this.remainingTimeMs = this.gameDurationMs;
        
        // Shuffle word list based on language (no duplicates)
        if (language === 'ko') {
            this.availableWords = getShuffledWordListKo();
        } else {
            this.availableWords = getShuffledWordList();
        }
        this.currentWordIndex = 0;
        console.log(`[Game] ${this.availableWords.length} unique words available`);
        
        // Hide start and game over screens
        this.startScreen.classList.remove('show');
        this.gameOverScreen.classList.remove('show');
        
        // Show game UI elements
        this.canvas.classList.add('active');
        document.getElementById('gameUI').classList.add('active');
        document.getElementById('inputDisplay').classList.add('active');
        
        // Clear hidden input and focus it
        this.hiddenInput.value = '';
        this.hiddenInput.focus();
        
        // Update UI
        this.updateUI();
        
        // Track game start
        tracker.trackGameStart();
        
        // Start game loop
        this.lastFrameTime = Date.now();
        this.gameLoop();
    }
    
    /**
     * Main game loop
     */
    gameLoop() {
        if (this.state !== 'running') return;
        
        const now = Date.now();
        const deltaTime = now - this.lastFrameTime;
        this.lastFrameTime = now;
        
        // Update timer
        this.remainingTimeMs = this.gameDurationMs - (now - this.gameStartTime);
        if (this.remainingTimeMs <= 0) {
            this.remainingTimeMs = 0;
            this.updateUI(); // Update UI to show 0
            this.gameOver('timeout');
            return;
        }
        
        // Spawn new words (rate increases over time)
        if (now - this.lastSpawnTime > this.currentSpawnInterval) {
            this.spawnWord();
            this.lastSpawnTime = now;
        }
        
        // Increase speed over time
        if (now - this.lastSpeedIncreaseTime > this.speedIncreaseInterval) {
            this.currentSpeed += this.speedIncrement;
            this.lastSpeedIncreaseTime = now;
            
            // Also decrease spawn interval (spawn more words)
            this.currentSpawnInterval = Math.max(
                this.minSpawnInterval,
                this.baseSpawnInterval - (this.currentSpeed - 1.0) * 200
            );
            
            console.log(`[Game] Speed: ${this.currentSpeed.toFixed(1)}x, Spawn: ${(this.currentSpawnInterval/1000).toFixed(1)}s`);
        }
        
        // Update falling words
        this.updateWords(deltaTime);
        
        // Render
        this.render();
        
        // Update UI
        this.updateUI();
        
        // Continue loop
        this.animationId = requestAnimationFrame(() => this.gameLoop());
    }
    
    /**
     * Spawn a new falling word
     */
    spawnWord() {
        // Check if all words have been used
        if (this.currentWordIndex >= this.availableWords.length) {
            console.log('[Game] All words completed!');
            this.gameOver('completed');
            return;
        }
        
        const word = this.availableWords[this.currentWordIndex];
        this.currentWordIndex++;
        
        // Calculate text width for proper offset
        this.ctx.font = '24px Courier New';
        const textMetrics = this.ctx.measureText(word);
        const textWidth = textMetrics.width;
        const margin = 20; // Extra padding from edges
        
        // Ensure word fits within canvas with margins
        const minX = textWidth / 2 + margin;
        const maxX = this.canvas.width - textWidth / 2 - margin;
        const x = Math.random() * (maxX - minX) + minX;
        
        const y = -20;
        const speed = this.baseSpeed * this.currentSpeed;
        
        const fallingWord = {
            word,
            x,
            y,
            speed,
            spawnTime: Date.now()
        };
        
        this.fallingWords.push(fallingWord);
        
        // Track word spawn
        tracker.trackWordSpawn(word, x, speed);
        
        console.log(`[Game] Spawned word: "${word}" at x=${x.toFixed(0)}`);
    }
    
    /**
     * Update all falling words
     */
    updateWords(deltaTime) {
        // Update positions
        for (let i = this.fallingWords.length - 1; i >= 0; i--) {
            const word = this.fallingWords[i];
            word.y += word.speed * (deltaTime / 16); // Normalize to ~60fps
            
            // Check if word reached bottom
            if (word.y > this.canvas.height) {
                // Track missed word
                tracker.trackWordMissed(word.word, this.currentSpeed);
                
                // Remove word
                this.fallingWords.splice(i, 1);
                this.wordsMissed++;
                
                console.log(`[Game] Word missed: "${word.word}"`);
                
                // Check game over condition (lose after 10 misses)
                if (this.wordsMissed >= 10) {
                    this.gameOver('missed');
                }
            }
        }
    }
    
    /**
     * Render game
     */
    render() {
        // Clear canvas
        this.ctx.fillStyle = 'rgba(0, 0, 0, 0.1)';
        this.ctx.fillRect(0, 0, this.canvas.width, this.canvas.height);
        
        // Draw falling words
        this.ctx.font = '24px Courier New';
        this.ctx.textAlign = 'center';
        
        for (const word of this.fallingWords) {
            // Check if this word matches current input (case-insensitive)
            const isPartialMatch = word.word.toLowerCase().startsWith(this.currentInput.toLowerCase()) && this.currentInput.length > 0;
            
            // Color based on match
            if (isPartialMatch) {
                this.ctx.fillStyle = '#00ff00';
                this.ctx.shadowColor = '#00ff00';
                this.ctx.shadowBlur = 10;
            } else {
                this.ctx.fillStyle = '#ffffff';
                this.ctx.shadowColor = 'transparent';
                this.ctx.shadowBlur = 0;
            }
            
            // Draw word
            this.ctx.fillText(word.word, word.x, word.y);
            
            // Draw progress indicator for partial match
            if (isPartialMatch) {
                const metrics = this.ctx.measureText(word.word);
                const progressWidth = metrics.width * (this.currentInput.length / word.word.length);
                this.ctx.fillStyle = 'rgba(0, 255, 0, 0.3)';
                this.ctx.fillRect(word.x - metrics.width / 2, word.y + 5, progressWidth, 3);
            }
        }
        
        // Reset shadow
        this.ctx.shadowBlur = 0;
    }
    
    /**
     * Handle letter input
     */
    handleLetterInput(letter) {
        // If starting new word, record start time
        if (this.currentInput.length === 0) {
            this.wordStartTime = Date.now();
        }
        
        this.currentInput += letter;
        this.updateInputDisplay();
        
        console.log(`[Game] Input: "${this.currentInput}"`);
    }
    
    /**
     * Handle backspace
     */
    handleBackspace() {
        if (this.currentInput.length > 0) {
            this.currentInput = this.currentInput.slice(0, -1);
            this.updateInputDisplay();
            
            // Reset word start time if cleared
            if (this.currentInput.length === 0) {
                this.wordStartTime = null;
            }
        }
    }
    
    /**
     * Handle submit (Enter key)
     */
    handleSubmit() {
        if (this.currentInput.length === 0) return;
        
        // Find matching word (case-insensitive)
        const matchIndex = this.fallingWords.findIndex(w => w.word.toLowerCase() === this.currentInput.toLowerCase());
        
        if (matchIndex !== -1) {
            // Correct!
            const matchedWord = this.fallingWords[matchIndex];
            const timeToType = Date.now() - this.wordStartTime;
            
            // Track correct typing
            tracker.trackWordTypedCorrect(matchedWord.word, timeToType, this.currentSpeed);
            
            // Update score (longer words = more points)
            const points = matchedWord.word.length * Math.floor(this.currentSpeed);
            this.score += points;
            this.wordsTyped++;
            
            // Remove word
            this.fallingWords.splice(matchIndex, 1);
            
            console.log(`[Game] Correct! "${this.currentInput}" (+${points} points, ${timeToType}ms)`);
        } else {
            // Incorrect
            const availableWords = this.fallingWords.map(w => w.word);
            tracker.trackWordTypedIncorrect(this.currentInput, availableWords);
            
            console.log(`[Game] Incorrect: "${this.currentInput}"`);
        }
        
        // Clear input
        this.currentInput = '';
        this.hiddenInput.value = '';
        this.wordStartTime = null;
        this.updateInputDisplay();
    }
    
    /**
     * Update input display
     */
    updateInputDisplay() {
        this.inputDisplay.textContent = this.currentInput || '\u00A0'; // Non-breaking space if empty
    }
    
    /**
     * Update UI displays
     */
    updateUI() {
        this.scoreDisplay.textContent = this.score;
        const timeSeconds = Math.max(0, Math.ceil(this.remainingTimeMs / 1000));
        this.timerDisplay.textContent = timeSeconds;
        this.speedDisplay.textContent = this.currentSpeed.toFixed(1) + 'x';
        this.missedDisplay.textContent = this.wordsMissed;
    }
    
    /**
     * Game over
     * @param {string} reason - 'timeout', 'missed', or 'completed'
     */
    gameOver(reason = 'timeout') {
        console.log(`[Game] Game Over - Reason: ${reason}`);
        
        this.state = 'game_over';
        
        // Stop animation
        if (this.animationId) {
            cancelAnimationFrame(this.animationId);
            this.animationId = null;
        }
        
        // Track game over
        tracker.trackGameOver(
            this.score,
            this.wordsTyped,
            this.wordsMissed,
            this.currentSpeed
        );
        
        // Show appropriate message based on reason
        const titleEl = document.getElementById('gameOverTitle');
        
        if (reason === 'completed') {
            titleEl.textContent = '🎉 Congratulations!';
            titleEl.style.color = '#00ff00';
        } else if (reason === 'timeout') {
            titleEl.textContent = '⏰ Time\'s Up!';
            titleEl.style.color = '#ff9500';
        } else {
            titleEl.textContent = '💀 Game Over';
            titleEl.style.color = '#ff5555';
        }
        
        // Update stats panel
        document.getElementById('finalScore').textContent = this.score;
        document.getElementById('finalTyped').textContent = this.wordsTyped;
        document.getElementById('finalMissed').textContent = this.wordsMissed;
        
        console.log('[Game] Showing game over screen');
        console.log('[Game] Score:', this.score, 'Typed:', this.wordsTyped, 'Missed:', this.wordsMissed);
        
        // Hide game UI
        this.canvas.classList.remove('active');
        document.getElementById('gameUI').classList.remove('active');
        document.getElementById('inputDisplay').classList.remove('active');
        
        // Show game over screen
        this.gameOverScreen.classList.add('show');
        
        console.log('[Game] Game over screen should now be visible');
    }
    
    /**
     * Restart game
     * @param {string} language - 'en' or 'ko'
     */
    restartGame(language = 'en') {
        this.startGame(language);
    }
}

// Initialize game when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    const game = new AcidRainGame();
    console.log('[Game] Ready to play!');
});
