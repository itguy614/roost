// @vitest-environment node
import { expect, it } from 'vitest'
import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

const css = readFileSync(resolve('src/theme/roost.css'), 'utf8')

function luminance(hex: string) {
  return (hex.match(/[^#]{2}/g) ?? []).reduce((total, value, i) => {
    const channel = parseInt(value, 16) / 255
    const linear = channel <= 0.04045 ? channel / 12.92 : ((channel + 0.055) / 1.055) ** 2.4
    return total + linear * [0.2126, 0.7152, 0.0722][i]
  }, 0)
}

it('keeps both palettes readable for text, status and keyboard focus', () => {
  const [light, dark] = [...css.matchAll(/html\.cui-theme-roost(?:\.dark)?\s*\{([^}]+)\}/g)].map((block) =>
    Object.fromEntries([...block[1].matchAll(/--cui-([a-z-]+):\s*(#[0-9a-f]+);/g)]
      .map(([, token, hex]) => [token, hex.length === 4 ? hex.replace(/[^#]/g, (c) => c + c) : hex])),
  )
  for (const tokens of [light, { ...light, ...dark }]) {
    function contrast(fg: string, bg: string, minimum: number) {
      expect(tokens[fg], fg).toBeDefined()
      expect(tokens[bg], bg).toBeDefined()
      const colors = [luminance(tokens[fg]), luminance(tokens[bg])].sort((a, b) => b - a)
      expect((colors[0] + 0.05) / (colors[1] + 0.05), `${fg} on ${bg}`).toBeGreaterThanOrEqual(minimum)
    }
    contrast('text-body', 'surface-base', 4.5)
    contrast('text-secondary', 'table-head-bg', 4.5)
    contrast('text-secondary', 'surface-light-bg', 4.5)
    contrast('primary', 'primary-bg', 4.5)
    contrast('primary-text', tokens['primary-solid'] ? 'primary-solid' : 'primary', 4.5)
    contrast('primary-focus-ring', 'surface-base', 3)
    contrast('primary-focus-ring', 'surface-light-bg', 3)
    for (const role of ['success', 'warning', 'error', 'info']) {
      contrast(role, `${role}-bg`, 4.5)
      contrast(`${role}-text`, role, 4.5)
    }
  }
})
