/* Diamond Wordle — UpperDecky
   Vanilla JS, no dependencies, no backend.
   -------- EDIT WORD LIST HERE (200 five-letter baseball words) -------- */
(function () {
  var WORDS_RAW = [
  "PITCH","SWING","CATCH","FIELD","PLATE","MOUND","BASES","CURVE","GLOVE","SPIKE",
  "CLEAT","CHALK","FENCE","GRASS","TRACK","HOMER","STEAL","SLIDE","SCORE","FORCE",
  "FOULS","BUNTS","BLAST","SMACK","CRACK","LINER","FLARE","DRIVE","POPUP","CHASE",
  "WHIFF","PAINT","BRUSH","FRAME","STUCK","OUTED","TAGED","RELAY","RALLY","SHORT",
  "THIRD","FIRST","COACH","SCOUT","CLOSR","SETUP","ROOKY","UTILY","SINKR","SLIDR",
  "SPLIT","KNUCK","CUTTR","HEATR","FORKN","CHNGE","STICK","LUMBR","MITTS","STRAP",
  "CHEST","HELMT","ROSIN","LACES","ALLEY","GAPER","TUNNL","STAND","DUGOT","RUBBR",
  "SLABS","INDEX","SPEED","ANGLE","RATIO","GRAPH","STATS","GRADE","LEVEL","PARKS",
  "TREND","RANGE","ERROR","CLEAN","SOLID","POWER","STREK","LATER","EARLY","DRAFT",
  "CHECK","JUMPS","LEAPS","TURNS","LUNGE","LIFTS","SHIFT","CROWD","CHANT","SWEEP",
  "TRADE","FUEGO","SMITE","FIRES","COUNT","BATCH","CARDS","PICKS","MATCH","TIMED",
  "READY","LOADS","READS","CARES","CLUBS","SQUAD","LEAGU","ROADS","HOMES","GUEST",
  "VISIT","LEADS","BEATS","TITLE","CHAMP","FINAL","MEDAL","TROPH","ROUTE","ROUND",
  "DROPS","LOSES","GAMES","ORBIT","BOUND","BRAVO","HEROS","ICING","KINGS","LASER",
  "MIGHT","NOBLY","OMEGA","PRIMA","QUOTA","RADAR","SPARK","THROW","UNITY","VIGOR",
  "WORKS","YELLS","ZEBRA","COBRA","FOXES","EAGLE","HAWKS","PUMAS","PANDA","RHINO",
  "SHARK","STAGS","TITAN","SWIFT","ROYAL","PIRAT","NOBLE","REBEL","SAVOR","RIVER",
  "SPORT","ARENA","BLOWS","BURNS","CLIMB","CUTTS","DANCE","DARTS","FAKED","FLOPS",
  "FRESH","GLIDE","GRIPS","GROWL","HOIST","HOMEY","HOPED","HOVER","HURLS","KICKS",
  "LAUNA","NOTCH","POISE","POINT","PRIDE","PROUD","PROVE","PULSE","RAPID","REACH"
  ];

  // Normalize: only 5-letter uppercase A-Z, dedupe
  var WORDS = (function () {
    var seen = {}, out = [];
    for (var i = 0; i < WORDS_RAW.length; i++) {
      var w = String(WORDS_RAW[i]).toUpperCase().replace(/[^A-Z]/g, '');
      if (w.length === 5 && !seen[w]) { seen[w] = 1; out.push(w); }
    }
    // safety fallback
    if (!out.length) out = ["BATTR","PITCH","SWING","CATCH","FIELD","PLATE","MOUND","BASES","CURVE","GLOVE"];
    return out;
  })();
  /* ---------------------------------------------------------------------- */

  var ROWS = 6, COLS = 5;
  var target = WORDS[Math.floor(Math.random() * WORDS.length)];
  var guesses = [];
  var current = "";
  var gameOver = false;

  var boardEl   = document.getElementById('bw-board');
  var statusEl  = document.getElementById('bw-status');
  var kbEl      = document.getElementById('bw-keyboard');
  var replayEl  = document.getElementById('bw-replay');

  function setStatus(text, cls) {
    statusEl.textContent = text || '\u00A0';
    statusEl.className = 'bw-status' + (cls ? ' ' + cls : '');
  }

  function evaluate(guess) {
    var result = new Array(COLS).fill('absent');
    var counts = {};
    for (var i = 0; i < COLS; i++) {
      if (guess[i] === target[i]) result[i] = 'correct';
      else counts[target[i]] = (counts[target[i]] || 0) + 1;
    }
    for (var j = 0; j < COLS; j++) {
      if (result[j] !== 'correct' && counts[guess[j]]) {
        result[j] = 'present';
        counts[guess[j]]--;
      }
    }
    return result;
  }

  function render() {
    boardEl.innerHTML = '';
    for (var r = 0; r < ROWS; r++) {
      var row = document.createElement('div');
      row.className = 'bw-row';
      var word, evalR;
      if (r < guesses.length) { word = guesses[r]; evalR = evaluate(word); }
      else if (r === guesses.length && !gameOver) { word = current.padEnd(COLS, ' '); evalR = null; }
      else { word = '     '; evalR = null; }
      for (var c = 0; c < COLS; c++) {
        var tile = document.createElement('div');
        tile.className = 'bw-tile';
        var ch = word[c];
        if (ch && ch !== ' ') {
          tile.textContent = ch;
          if (evalR) tile.classList.add(evalR[c]);
          else tile.classList.add('filled');
        }
        row.appendChild(tile);
      }
      boardEl.appendChild(row);
    }
    renderKeyboard();
    replayEl.hidden = !gameOver;
  }

  function renderKeyboard() {
    var layout = [
      ['Q','W','E','R','T','Y','U','I','O','P'],
      ['A','S','D','F','G','H','J','K','L'],
      ['ENTER','Z','X','C','V','B','N','M','BACK']
    ];
    var keyState = {};
    for (var i = 0; i < guesses.length; i++) {
      var g = guesses[i], ev = evaluate(g);
      for (var j = 0; j < g.length; j++) {
        var letter = g[j], st = ev[j], prev = keyState[letter];
        if (!prev || (prev === 'absent') || (prev === 'present' && st === 'correct')) keyState[letter] = st;
      }
    }
    kbEl.innerHTML = '';
    layout.forEach(function (row) {
      var rowEl = document.createElement('div');
      rowEl.className = 'bw-kbrow';
      row.forEach(function (k) {
        var btn = document.createElement('button');
        btn.className = 'bw-key';
        if (k === 'ENTER' || k === 'BACK') btn.classList.add('wide');
        if (keyState[k]) btn.classList.add(keyState[k]);
        btn.textContent = k === 'BACK' ? '\u232B' : k;
        btn.setAttribute('data-key', k);
        btn.addEventListener('click', function (ev) { ev.preventDefault(); handleKey(k); });
        rowEl.appendChild(btn);
      });
      kbEl.appendChild(rowEl);
    });
  }

  function handleKey(key) {
    if (gameOver) return;
    if (key === 'ENTER') return submit();
    if (key === 'BACK')  { current = current.slice(0, -1); setStatus(''); render(); return; }
    if (/^[A-Z]$/.test(key) && current.length < COLS) {
      current += key;
      setStatus('');
      render();
    }
  }

  function submit() {
    if (current.length !== COLS) { setStatus('Need 5 letters.', 'err'); return; }
    guesses.push(current);
    if (current === target) {
      setStatus('\u26BE Nice swing! You got it.', 'win');
      gameOver = true;
    } else if (guesses.length >= ROWS) {
      setStatus('Struck out. Word was ' + target, 'loss');
      gameOver = true;
    } else {
      setStatus('');
    }
    current = '';
    render();
  }

  function playAgain() {
    target = WORDS[Math.floor(Math.random() * WORDS.length)];
    guesses = []; current = ''; gameOver = false;
    setStatus(''); render();
  }

  document.addEventListener('keydown', function (e) {
    if (gameOver) return;
    if (e.key === 'Enter') { e.preventDefault(); submit(); return; }
    if (e.key === 'Backspace') { e.preventDefault(); handleKey('BACK'); return; }
    var k = e.key.toUpperCase();
    if (/^[A-Z]$/.test(k)) { e.preventDefault(); handleKey(k); }
  });
  replayEl.addEventListener('click', playAgain);

  render();
})();
