import { setImmediate as flushPromises } from 'node:timers/promises'
import { invoke } from '@tauri-apps/api/core'
import { listen, type Event } from '@tauri-apps/api/event'
import { createPinia, setActivePinia } from 'pinia'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createApp } from 'vue'
import App from './App.vue'
import { useDashboard, type Dashboard } from './dashboard'

vi.mock('@tauri-apps/api/core', () => ({ invoke: vi.fn() }))
vi.mock('@tauri-apps/api/event', () => ({ listen: vi.fn() }))

const empty: Dashboard = {
  daemon: { pid: 12, protocol_version: 1, uptime_seconds: 4 },
  projects: [],
  page: 0,
  total: 0,
  has_next: false,
}

const populated: Dashboard = {
  ...empty,
  total: 1,
  projects: [{
    id: 'project',
    name: 'Fulfillment',
    path: '/tmp/Fulfillment',
    desired_state: 'running',
    observed: {
      state: 'available',
      summary: {
        workers: { desired: 2, running: 1, unresolved: 1 },
        scheduler: { desired: 1, running: 1, unresolved: 0 },
      },
    },
  }],
}

function deferredSnapshot() {
  let resolve!: (snapshot: Dashboard) => void
  const promise = new Promise<Dashboard>((done) => { resolve = done })
  return { promise, resolve }
}

function mountShell() {
  const root = document.createElement('div')
  document.body.append(root)
  const app = createApp(App).use(createPinia())
  app.mount(root)
  return {
    root,
    find<T extends HTMLElement = HTMLElement>(selector: string): T {
      const element = root.querySelector<T>(selector)
      if (!element) throw new Error(`Missing ${selector}`)
      return element
    },
    unmount() {
      app.unmount()
      root.remove()
    },
  }
}

const ipc = vi.mocked(invoke)
const nativeListen = vi.mocked(listen)

beforeEach(() => {
  vi.resetAllMocks()
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] })
  setActivePinia(createPinia())
})

afterEach(() => {
  vi.useRealTimers()
})

describe('shared dashboard reads', () => {
  it('retries after disconnection and retains the latest good snapshot', async () => {
    ipc.mockResolvedValueOnce(populated)
      .mockRejectedValueOnce('disconnected')
      .mockRejectedValueOnce('timeout')
      .mockResolvedValueOnce(empty)
    const store = useDashboard()
    store.setActive(true)
    await flushPromises()
    expect(store.snapshot).toEqual(populated)
    await vi.advanceTimersByTimeAsync(5_000)
    expect(store.fault).toBe('disconnected')
    expect(store.snapshot).toEqual(populated)
    await vi.advanceTimersByTimeAsync(2_000)
    expect(store.fault).toBe('timeout')
    await vi.advanceTimersByTimeAsync(3_999)
    expect(ipc).toHaveBeenCalledTimes(3)
    await vi.advanceTimersByTimeAsync(1)
    expect(store.fault).toBeNull()
    expect(store.snapshot).toEqual(empty)
    store.setActive(false)
    await vi.advanceTimersByTimeAsync(60_000)
    expect(ipc).toHaveBeenCalledTimes(4)
  })

  it('coalesces refreshes and drops an obsolete page response across hide and reopen', async () => {
    const before = deferredSnapshot()
    const after = deferredSnapshot()
    ipc.mockReturnValueOnce(before.promise).mockReturnValueOnce(after.promise)
    const store = useDashboard()
    store.setActive(true)
    store.refresh()
    store.selectPage(1)
    store.setActive(false)
    store.setActive(true)
    expect(ipc).toHaveBeenCalledTimes(1)
    before.resolve(populated)
    await flushPromises()
    expect(store.snapshot).toBeNull()
    expect(ipc).toHaveBeenLastCalledWith('dashboard_snapshot', { page: 1 })
    after.resolve({ ...empty, page: 1 })
    await flushPromises()
    expect(store.snapshot?.page).toBe(1)
    store.setActive(false)
  })
})

describe('desktop shell', () => {
  it('shows recovery blocked, gives navigation focus, and suspends when hidden', async () => {
    let setVisible!: (event: Event<boolean>) => void
    const stop = vi.fn()
    nativeListen.mockImplementation(async (_event, handler) => {
      setVisible = handler as (event: Event<boolean>) => void
      return stop
    })
    ipc.mockResolvedValue(populated)
    const app = mountShell()
    await flushPromises()
    expect(app.find('table').textContent).toContain('Recovery blocked')
    app.find('.project-link').click()
    await flushPromises()
    expect(app.find('h1').textContent).toBe('Fulfillment')
    expect(document.activeElement).toBe(app.find('h1'))
    expect(app.find('nav button[aria-current]').textContent).toBe('Fulfillment')

    setVisible({ event: 'roost://window-visible', id: 1, payload: false })
    await vi.advanceTimersByTimeAsync(60_000)
    expect(ipc).toHaveBeenCalledTimes(1)
    setVisible({ event: 'roost://window-visible', id: 1, payload: true })
    await flushPromises()
    expect(ipc).toHaveBeenCalledTimes(2)
    app.unmount()
    expect(stop).toHaveBeenCalledOnce()
  })

  it('renders offline then empty after a real retry action, without invented counts', async () => {
    nativeListen.mockResolvedValue(() => {})
    ipc.mockRejectedValueOnce('offline').mockResolvedValueOnce(empty)
    const app = mountShell()
    expect(app.root.textContent).toContain('Connecting to the Roost daemon')
    await flushPromises()
    expect(app.find('.notice').textContent).toContain('Roost daemon is offline')
    expect(app.root.querySelector('table')).toBeNull()
    app.find('.notice button').click()
    await flushPromises()
    expect(app.find('.empty').textContent).toContain('No projects yet')
    app.unmount()
  })
})
