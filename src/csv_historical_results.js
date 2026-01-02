function normalizeNewlines(s) {
	return String(s ?? '').replaceAll('\r\n', '\n').replaceAll('\r', '\n');
}

function isNumericLike(v) {
	const s = String(v ?? '').trim();
	return s !== '' && /^-?\d+(?:\.\d+)?$/.test(s);
}

function parseTempFString(v) {
	const s = String(v ?? '').trim();
	const m = s.match(/-?\d+(?:\.\d+)?/);
	if (!m) return null;
	const n = parseFloat(m[0]);
	return Number.isFinite(n) ? n : null;
}

function parseMaybeInt(v) {
	const s = String(v ?? '').trim();
	if (!s) return null;
	const n = parseInt(s, 10);
	return Number.isFinite(n) ? n : null;
}

function parseMaybeFloat(v) {
	const s = String(v ?? '').trim();
	if (!s) return null;
	const n = parseFloat(s);
	return Number.isFinite(n) ? n : null;
}

function parseLatLong(latLongStr) {
	const s = String(latLongStr ?? '').trim();
	if (!s || !s.includes('/')) return { Lat: null, Lon: null };
	const [latRaw, lonRaw] = s.split('/');
	const lat = parseFloat(String(latRaw ?? '').trim());
	const lon = parseFloat(String(lonRaw ?? '').trim());
	return {
		Lat: Number.isFinite(lat) ? lat : null,
		Lon: Number.isFinite(lon) ? lon : null,
	};
}

export function parseCsvText(text, { maxRows = 250000, maxCols = 200 } = {}) {
	const s = normalizeNewlines(text);

	const rows = [];
	let row = [];
	let field = '';
	let inQuotes = false;

	const pushField = () => {
		row.push(field);
		field = '';
	};
	const pushRow = () => {
		while (row.length && String(row[row.length - 1] ?? '').trim() === '') row.pop();
		if (row.length) rows.push(row);
		row = [];
	};

	for (let i = 0; i < s.length; i++) {
		const ch = s[i];

		if (inQuotes) {
			if (ch === '"') {
				const next = s[i + 1];
				if (next === '"') {
					field += '"';
					i++;
				} else {
					inQuotes = false;
				}
			} else {
				field += ch;
			}
			continue;
		}

		if (ch === '"') {
			inQuotes = true;
			continue;
		}

		if (ch === ',') {
			pushField();
			if (row.length > maxCols) throw new Error(`CSV has too many columns (>${maxCols}).`);
			continue;
		}

		if (ch === '\n') {
			pushField();
			pushRow();
			if (rows.length > maxRows) throw new Error(`CSV has too many rows (>${maxRows}).`);
			continue;
		}

		field += ch;
	}

	pushField();
	pushRow();

	return rows;
}

function parseYearHeaderCell(cell) {
	const s = normalizeNewlines(cell);
	const head = s.split('\n')[0] || '';
	const m = head.match(/^\s*(19\d{2}|20\d{2})\s*\[(\d+)\]/);
	if (!m) return null;
	const Year = parseInt(m[1], 10);
	const CountIndex = parseInt(m[2], 10);

	const dm = s.match(/Count Date:\s*([0-9]{1,2}\/[0-9]{1,2}\/[0-9]{4})/);
	const pm = s.match(/#\s*Participants:\s*([0-9]+)/);
	const sm = s.match(/#\s*Species Reported:\s*([0-9]+)/);
	const hm = s.match(/Total Hrs\.:\s*([0-9]+(?:\.[0-9]+)?)/);

	return {
		Year: Number.isFinite(Year) ? Year : null,
		CountIndex: Number.isFinite(CountIndex) ? CountIndex : null,
		CountDate: dm ? dm[1] : null,
		NumParticipants: pm ? parseInt(pm[1], 10) : null,
		NumSpeciesReported: sm ? parseInt(sm[1], 10) : null,
		TotalHrs: hm ? parseFloat(hm[1]) : null,
	};
}

function uniqueSortedYearsFromMeta(meta) {
	const set = new Set();
	for (const r of meta || []) {
		if (typeof r?.Year === 'number' && Number.isFinite(r.Year)) set.add(r.Year);
	}
	return Array.from(set).sort((a, b) => a - b);
}

export function parseHistoricalResultsByCountCsv(text) {
	const rows = parseCsvText(text, { maxRows: 250000, maxCols: 200 });

	const countInfo = {
		CountName: null,
		CountCode: null,
		CountId: null,
		Lat: null,
		Lon: null,
		CompilerFirstName: null,
		CompilerLastName: null,
		CompilerName: null,
		CompilerEmail: null,
	};

	const norm = (v) => String(v ?? '').replace(/\s+/g, ' ').trim();
	const firstNonEmptyAfter = (r, idx) => {
		for (let j = idx + 1; j < (r || []).length; j++) {
			const s = norm(r[j]);
			if (s) return s;
		}
		return null;
	};

	for (let i = 0; i < Math.min(rows.length, 80); i++) {
		const r = rows[i] || [];
		if (!Array.isArray(r) || r.length === 0) continue;

		const headerIndexes = new Map();
		for (let j = 0; j < r.length; j++) {
			const key = norm(r[j]).toLowerCase();
			if (!key) continue;
			if (key === 'compilerfirstname') headerIndexes.set('first', j);
			if (key === 'compilerlastname') headerIndexes.set('last', j);
			if (key === 'compileremail' || key === 'email') headerIndexes.set('email', j);
			if (key === 'compilername' || key === 'compiler') headerIndexes.set('name', j);
		}
		if (headerIndexes.size) {
			const v = rows[i + 1] || [];
			if (headerIndexes.has('first')) countInfo.CompilerFirstName = norm(v[headerIndexes.get('first')]) || countInfo.CompilerFirstName;
			if (headerIndexes.has('last')) countInfo.CompilerLastName = norm(v[headerIndexes.get('last')]) || countInfo.CompilerLastName;
			if (headerIndexes.has('email')) countInfo.CompilerEmail = norm(v[headerIndexes.get('email')]) || countInfo.CompilerEmail;
			if (headerIndexes.has('name')) countInfo.CompilerName = norm(v[headerIndexes.get('name')]) || countInfo.CompilerName;
		}

		for (let j = 0; j < r.length; j++) {
			const cell = norm(r[j]);
			if (!cell) continue;
			const lower = cell.toLowerCase();

			if (lower.includes('compiler email') || (lower.includes('compiler') && lower.includes('email'))) {
				const inline = cell.split(':').slice(1).join(':').trim();
				const v = inline || firstNonEmptyAfter(r, j);
				if (v) countInfo.CompilerEmail = v;
				continue;
			}

			if (lower.startsWith('compiler') || lower.startsWith('current compiler')) {
				const inline = cell.split(':').slice(1).join(':').trim();
				const v = inline || firstNonEmptyAfter(r, j);
				if (v) countInfo.CompilerName = v;
				continue;
			}
		}
	}

	for (let i = 0; i < Math.min(rows.length, 50); i++) {
		const r = rows[i] || [];
		if (r[0] === 'CircleName' && r[1] === 'Abbrev') {
			const v = rows[i + 1] || [];
			countInfo.CountName = String(v[0] ?? '').trim() || null;
			countInfo.CountCode = String(v[1] ?? '').trim() || null;
			const ll = parseLatLong(v[2]);
			countInfo.Lat = ll.Lat;
			countInfo.Lon = ll.Lon;
			break;
		}
	}

	const weatherRaw = [];

	const participantsEffort = [];

	const meta = [];
	const metaSeen = new Set();

	for (let i = 0; i < rows.length; i++) {
		const r = rows[i] || [];
		if (r[0] === 'CountYear3' && (r[1] || '').toLowerCase().includes('lowtemp')) {
			for (let j = i + 1; j < rows.length; j++) {
				const d = rows[j] || [];
				if (!d.length) break;
				if (!isNumericLike(d[0])) break;
				const CountIndex = parseMaybeInt(d[0]);
				if (!CountIndex) continue;
				weatherRaw.push({
					CountIndex,
					LowTempF: parseTempFString(d[1]),
					HighTempF: parseTempFString(d[2]),
					AMClouds: String(d[3] ?? ''),
					PMClouds: String(d[4] ?? ''),
					AMRain: String(d[5] ?? ''),
					PMRain: String(d[6] ?? ''),
					AMSnow: String(d[7] ?? ''),
					PMSnow: String(d[8] ?? ''),
				});
			}
			break;
		}
	}

	for (let i = 0; i < rows.length; i++) {
		const r = rows[i] || [];
		if (r[0] === 'CountYear5') {
			for (let j = i + 1; j < rows.length; j++) {
				const d = rows[j] || [];
				if (!d.length) break;
				if (!isNumericLike(d[0])) break;
				const CountIndex = parseMaybeInt(d[0]);
				if (!CountIndex) continue;

				const CountDate = String(d[1] ?? '').trim() || null;
				participantsEffort.push({
					CountIndex,
					CountDate,
					Year: null,
					NumParticipants: parseMaybeInt(d[2]),
					NumHours: parseMaybeFloat(d[3]),
					NumSpeciesReported: parseMaybeInt(d[4]),
				});
			}
			break;
		}
	}

	let speciesSectionStart = -1;
	for (let i = 0; i < rows.length; i++) {
		const r = rows[i] || [];
		if (r[0] === 'COM_NAME' && r[1] === 'CountYear') {
			speciesSectionStart = i + 1;
			break;
		}
	}

	const countsBySpecies = new Map();

	if (speciesSectionStart !== -1) {
		for (let j = speciesSectionStart; j < rows.length; j++) {
			const d = rows[j] || [];
			if (!d.length) break;

			const speciesRaw = String(d[0] ?? '').trim();
			const hdr = d[1];
			if (!speciesRaw || !hdr) continue;

			const yh = parseYearHeaderCell(hdr);
			if (!yh || yh.CountIndex === null || yh.Year === null) continue;

			const CountIndex = yh.CountIndex;
			const Year = yh.Year;
			const CountDate = yh.CountDate;

			if (!metaSeen.has(CountIndex)) {
				metaSeen.add(CountIndex);
				meta.push({ CountIndex, Year, CountDate: CountDate || null });
			}

			const existing = participantsEffort.find((r) => r.CountIndex === CountIndex);
			if (existing) {
				if (!existing.CountDate && CountDate) existing.CountDate = CountDate;
				if (existing.NumParticipants === null && yh.NumParticipants !== null) existing.NumParticipants = yh.NumParticipants;
				if (existing.NumSpeciesReported === null && yh.NumSpeciesReported !== null) existing.NumSpeciesReported = yh.NumSpeciesReported;
				if (existing.NumHours === null && yh.TotalHrs !== null) existing.NumHours = yh.TotalHrs;
			}

			const species = speciesRaw.split('\n')[0].trim();
			const countVal = parseMaybeInt(d[2]) ?? 0;
			let m = countsBySpecies.get(species);
			if (!m) {
				m = new Map();
				countsBySpecies.set(species, m);
			}
			m.set(Year, countVal);
		}
	}

	const years = uniqueSortedYearsFromMeta(meta);
	const speciesTable = [];
	for (const [species, byYear] of countsBySpecies.entries()) {
		const rec = { Species: species };
		for (const y of years) {
			if (byYear.has(y)) rec[String(y)] = byYear.get(y);
		}
		speciesTable.push(rec);
	}

	return {
		countInfo,
		meta,
		participantsEffort,
		weatherRaw,
		speciesTable,
		years,
	};
}
