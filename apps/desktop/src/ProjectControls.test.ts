import { createCleanUI } from '@itguy614/clean-ui'
import { afterEach, expect, it, vi } from 'vitest'
import { createApp } from 'vue'
import ProjectControls from './ProjectControls.vue'
import type { ProjectRow } from './dashboard'
import { type ProjectDetail, type Slot, useProjectOperations } from './projectOperations'

const project: ProjectRow = {
  id: 'dispatch', name: 'Dispatch', path: '/tmp/Dispatch',
  desired_state: 'running', observed: { state: 'unavailable' },
}
const profile = {
  id: 'profile', name: 'Default', connection: 'database', queues: [], desired_instances: 1, enabled: true,
}
const capabilities = [
  'provider.resolvePhp', 'project.start', 'project.stop', 'project.restart',
  'scheduler.start', 'scheduler.stop', 'scheduler.restart', 'project.remove',
]

let unmount: (() => void) | undefined

function mount(workers: Slot[] | null, process: Slot | null, desired: 'stopped' | 'running' = 'running') {
  const root = document.createElement('div')
  document.body.append(root)
  const detail: ProjectDetail = {
    watch_enabled: false, connections: null, revision: 1, provider: 'system',
    autostart: false, config_owner: 'local', profiles: [profile], workers,
    scheduler: { configured: true, enabled: true, desired_state: desired, process },
    watch: null, capabilities,
  }
  const operations = useProjectOperations(vi.fn(), vi.fn())
  const app = createApp(ProjectControls, { project: { ...project, desired_state: desired }, detail, locked: false, operations })
  app.use(createCleanUI()).mount(root)
  unmount = () => { app.unmount(); root.remove() }
  return {
    text: root.textContent,
    button: (name: string) => [...root.querySelectorAll('button')].find((item) => item.textContent?.trim() === name),
  }
}
afterEach(() => unmount?.())

it.each([null, []])('allows Stop based on running intent even without worker slots or a scheduler process (%j)', (slots) => {
  const view = mount(slots, null)
  expect(view.button('Stop workers')?.disabled).toBe(false)
  expect(view.button('Stop scheduler')?.disabled).toBe(false)
  expect(view.button('Start workers')).toBeUndefined()
  expect(view.button('Start scheduler')).toBeUndefined()
  expect(view.button('Remove project')?.disabled).toBe(true)
  expect(view.text).toMatch(/Worker observations are unavailable|No observed worker slots/)
  expect(view.text).toContain('No observed process')
})

it.each(['starting', 'stopping', 'backoff', 'failed'] as const)('keeps Stop available during %s and gates restart independently', (state) => {
  const slot = { id: 'slot', state, recovery_blocked: false }
  const view = mount([slot], slot)
  expect(view.button('Stop workers')?.disabled).toBe(false)
  expect(view.button('Stop scheduler')?.disabled).toBe(false)
  expect(view.button('Restart workers')?.disabled).toBe(state === 'starting' || state === 'stopping')
  expect(view.button('Restart scheduler')?.disabled).toBe(state === 'starting' || state === 'stopping')
  expect(view.text).toContain(state)
})

it('shows an unresolved recovery block and disables launches without removing Stop', () => {
  const blocked: Slot = { id: 'slot', state: 'failed', recovery_blocked: true }
  const view = mount([blocked], blocked)
  expect(view.button('Restart workers')?.disabled).toBe(true)
  expect(view.button('Restart scheduler')?.disabled).toBe(true)
  expect(view.button('Stop workers')?.disabled).toBe(false)
  expect(view.button('Stop scheduler')?.disabled).toBe(false)
  expect(view.text).toMatch(/Worker launch is blocked/)
  expect(view.text).toMatch(/Scheduler launch is blocked/)
})
