# Spanish DNI/NIE Validation Rules

## DNI (Documento Nacional de Identidad)

**Format**: 8 digits + 1 control letter (e.g., `12345678Z`)

### Control Letter Algorithm
1. Take the 8-digit number
2. Compute `number % 23`
3. Look up the letter in the table:

| Remainder | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 |
|-----------|---|---|---|---|---|---|---|---|---|---|----|----|
| Letter    | T | R | W | A | G | M | Y | F | P | D | X  | B  |

| Remainder | 12 | 13 | 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 22 |
|-----------|----|----|----|----|----|----|----|----|----|----|------|
| Letter    | N  | J  | Z  | S  | Q  | V  | H  | L  | C  | K  | E  |

**Full letter string**: `TRWAGMYFPDXBNJZSQVHLCKE`

### Example
- DNI number: `12345678`
- `12345678 % 23 = 14`
- Letter at position 14 = `Z`
- Valid DNI: `12345678Z`

## NIE (Número de Identidad de Extranjero)

**Format**: 1 letter prefix (X, Y, or Z) + 7 digits + 1 control letter (e.g., `X1234567L`)

### Control Letter Algorithm
1. Replace the prefix letter with a digit: `X=0`, `Y=1`, `Z=2`
2. Concatenate to form an 8-digit number
3. Apply the same modulo-23 algorithm as DNI

### Example
- NIE: `X1234567`
- Replace X → 0: `01234567`
- `1234567 % 23 = 20`
- Letter at position 20 = `C`
- Valid NIE: `X1234567C`

## Common Issues Found in Data
- Missing control letter
- Lowercase letters (should be uppercase)
- Spaces or dashes in the middle
- Leading zeros missing (e.g., `1234567Z` instead of `01234567Z`)
- Old-format NIE with prefix letter not replaced in calculation
- CIF/NIF business numbers mixed with personal DNIs

## Pre-processing Before Validation
1. Convert to uppercase
2. Remove spaces, dashes, dots
3. Trim whitespace
4. Then validate format and control letter
