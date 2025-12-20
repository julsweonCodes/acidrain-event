/**
 * Hardcoded word dataset for the typing game
 * This list is intentionally small and focused for event generation
 * Words can repeat during gameplay
 */

const WORD_LIST = [
    // Common programming terms
    "function", "variable", "constant", "array", "object",
    "string", "number", "boolean", "null", "undefined",
    "class", "method", "property", "interface", "type",
    "async", "await", "promise", "callback", "event",
    "loop", "condition", "switch", "case", "break",
    
    // Short words for speed
    "code", "bug", "fix", "test", "run",
    "git", "push", "pull", "merge", "branch",
    "data", "json", "api", "http", "rest",
    
    // Medium difficulty
    "database", "query", "index", "schema", "table",
    "pipeline", "stream", "buffer", "cache", "token",
    "session", "cookie", "header", "payload", "response",
    
    // Longer challenging words
    "authentication", "authorization", "middleware", "configuration",
    "deployment", "repository", "implementation", "infrastructure",
    "optimization", "architecture", "documentation", "integration",
    
    // Analytics-specific terms
    "metric", "event", "track", "analyze", "measure",
    "dimension", "aggregate", "filter", "segment", "cohort",
    "retention", "conversion", "funnel", "engagement", "attribution",
    
    // General tech terms
    "server", "client", "request", "endpoint", "route",
    "component", "module", "package", "library", "framework",
    "algorithm", "parameter", "argument", "return", "export",
    
    // German/Foreign loanwords (challenging)
    "zeitgeist", "schadenfreude", "weltschmerz", "doppelganger", "gestalt",
    "kindergarten", "poltergeist", "wanderlust", "angst", "verboten",
    "kaput", "blitzkrieg", "fahrenheit", "rucksack", "sauerkraut",
    
    // French loanwords
    "bourgeoisie", "entrepreneur", "renaissance", "rendezvous", "repertoire",
    "chauffeur", "silhouette", "questionnaire", "connoisseur", "surveillance",
    "bureaucracy", "ensemble", "critique", "sabotage", "camouflage",
    
    // Latin/Greek terms
    "phenomenon", "epitome", "euphemism", "hypothesis", "paradigm",
    "metamorphosis", "asymmetric", "catastrophe", "chromosome", "diameter",
    "encyclopedia", "gymnasium", "hierarchy", "atmosphere", "philosophy",
    
    // Complex English words (difficult spelling/typing)
    "accommodate", "necessary", "definitely", "separate", "maintenance",
    "correspondence", "conscience", "rhythm", "guarantee", "embarrass",
    "occurrence", "possession", "privilege", "pronunciation", "recommend",
    "millennium", "questionnaire", "rhythm", "bureaucracy", "harassment",
    
    // Technical/specialized terms
    "asymptotic", "polymorphism", "encapsulation", "isomorphic", "idempotent",
    "concurrent", "synchronous", "asynchronous", "heuristic", "stochastic",
    "heterogeneous", "homogeneous", "parallelism", "virtualization", "orchestration",
    "cryptography", "steganography", "biometric", "quantum", "algorithm",
    
    // Words with tricky letter patterns
    "bookkeeper", "commitment", "committee", "Massachusetts", "Mississippi",
    "Tennessee", "Connecticut", "acquaintance", "bureaucratic", "conscientious",
    "fluorescent", "incandescent", "phosphorescent", "acknowledgment", "accidentally",
    
    // Scientific/Medical term
    "chromosome", "mitochondrion", "photosynthesis", "glycolysis", "homeostasis",
    "neurotransmitter", "deoxyribonucleic", "electromagnetic", "thermodynamic", "equilibrium",
    "metabolism", "pharmaceutical", "diagnosis", "prognosis", "synthesis",
    
    // Compound challenging word
    "juxtaposition", "onomatopoeia", "antidisestablishmentarianism", "pseudopseudohypoparathyroidism",
    "floccinaucinihilipilification", "hippopotomonstrosesquippedaliophobia", "pneumonoultramicroscopicsilicovolcanoconiosis",
    "supercalifragilisticexpialidocious", "incomprehensibility", "counterrevolutionary"
];

/**
 * Get a random word from the dataset
 * @returns {string} Random word
 */
function getRandomWord() {
    return WORD_LIST[Math.floor(Math.random() * WORD_LIST.length)];
}

/**
 * Get multiple random words
 * @param {number} count - Number of words to get
 * @returns {string[]} Array of random words
 */
function getRandomWords(count) {
    return Array.from({ length: count }, () => getRandomWord());
}

/**
 * Get a shuffled copy of the word list (no duplicates)
 * @returns {string[]} Shuffled array of all words
 */
function getShuffledWordList() {
    const shuffled = [...WORD_LIST];
    for (let i = shuffled.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
    }
    return shuffled;
}
