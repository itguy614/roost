import { createCleanUI } from '@itguy614/clean-ui'
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
  pages: 1,
  directory: [],
  selected: null,
}

const populated: Dashboard = {
  ...empty,
  total: 1,
  directory: [{ id: 'project', name: 'Fulfillment' }],
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
  const app = createApp(App).use(createPinia()).use(createCleanUI())
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
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] })
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
    expect(ipc).toHaveBeenLastCalledWith('dashboard_snapshot', { page: 1, selectedId: null })
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
    await vi.advanceTimersByTimeAsync(1)
    app.find('.project-link').click()
    await flushPromises()
    expect(app.find('h1').textContent).toBe('Fulfillment')
    expect(document.activeElement).toBe(app.find('h1'))
    expect(app.find('nav button[aria-current]').textContent).toBe('Fulfillment')

    setVisible({ event: 'roost://window-visible', id: 1, payload: false })
    await vi.advanceTimersByTimeAsync(60_000)
    expect(ipc).toHaveBeenCalledTimes(2)
    setVisible({ event: 'roost://window-visible', id: 1, payload: true })
    await flushPromises()
    expect(ipc).toHaveBeenCalledTimes(3)
    app.unmount()
    expect(stop).toHaveBeenCalledOnce()
  })

  it('renders offline then empty after a real retry action, without invented counts', async () => {
    nativeListen.mockResolvedValue(() => {})
    ipc.mockRejectedValueOnce('offline').mockResolvedValueOnce(empty)
    const app = mountShell()
    expect(app.root.textContent).toContain('Checking daemon connection')
    await flushPromises()
    expect(app.find('.view-notice').textContent).toContain('Roost daemon is offline')
    expect(app.root.querySelector('table')).toBeNull()
    const retry = app.find('.view-notice button')
    expect(retry.hasAttribute('aria-disabled')).toBe(false)
    // Let the event time advance past Vue's capture and bubble listener attachment time.
    await vi.advanceTimersByTimeAsync(1)
    retry.click()
    expect(ipc).toHaveBeenCalledTimes(2)

    await flushPromises()
    expect(app.find('.cui-empty-state').textContent).toContain('No projects yet')
    app.unmount()
  })

  it('marks cached observations stale and checks connectivity immediately after reopening', async () => {
    let setVisible!: (event: Event<boolean>) => void
    nativeListen.mockImplementation(async (_event, handler) => {
      setVisible = handler as (event: Event<boolean>) => void
      return () => {}
    })
    const after = deferredSnapshot()
    ipc.mockResolvedValueOnce(populated).mockReturnValueOnce(after.promise)
    const app = mountShell()
    await flushPromises()
    const updated = app.find('.freshness').textContent?.match(/at (.*)/)?.[1]?.trim()
    expect(updated).toBeTruthy()
    expect(app.find('.sidebar-status').textContent).toContain('Daemon connected')
    setVisible({ event: 'roost://window-visible', id: 1, payload: false })
    setVisible({ event: 'roost://window-visible', id: 1, payload: true })
    await flushPromises()
    expect(app.find('.sidebar-status').textContent).toContain('Checking connection')
    expect(app.find('.freshness').textContent).toContain(`Stale snapshot from ${updated}`)
    expect(app.find('table').textContent).toContain('Fulfillment')
    after.resolve(populated)
    await flushPromises()
    expect(app.find('.freshness').textContent).toContain('Last updated at')
    expect(app.find('.sidebar-status').textContent).toContain('Daemon connected')
    app.unmount()
  })

  it('keeps off-page projects navigable and describes count discrepancies without inventing a lifecycle state', async () => {
    const entry = { id: 'elsewhere', name: 'Off-page project' }
    const row = {
      ...populated.projects[0],
      observed: {
        state: 'available',
        summary: {
          workers: { desired: 2, running: 1, unresolved: 0 },
          scheduler: { desired: 0, running: 1, unresolved: 0 },
        },
      },
    } satisfies Dashboard['projects'][number]
    const first = { ...populated, pages: 2, total: 9, directory: [...populated.directory, entry], projects: [row] }
    const selected = { ...row, ...entry }
    nativeListen.mockResolvedValue(() => {})
    ipc.mockResolvedValueOnce(first)
      .mockResolvedValueOnce({ ...first, selected })
      .mockResolvedValueOnce(first)
      .mockResolvedValueOnce({ ...first, page: 1, projects: [selected] })
    const app = mountShell()
    await flushPromises()
    expect(app.find('th[scope="col"]').textContent).toBe('Project')
    expect(app.find('caption').textContent).toContain('Registered projects')
    const table = app.find('table').textContent
    expect(table).toContain('Running requested')
    expect(table).toContain('Below desired')
    expect(table).toContain('Above desired')
    expect(table).not.toContain('Recovery blocked')
    const nav = app.find('nav[aria-label="Roost sidebar"]')
    const choose = () => [...nav.querySelectorAll('button')].find((button) => button.textContent === entry.name)
    expect(choose()).toBeDefined()
    await vi.advanceTimersByTimeAsync(1)
    choose()?.click()
    await flushPromises()
    expect(ipc).toHaveBeenLastCalledWith('dashboard_snapshot', { page: 0, selectedId: 'elsewhere' })
    expect(app.find('h1').textContent).toBe(entry.name)
    expect(app.find('.project-detail').textContent).toContain('Above desired')
    await vi.advanceTimersByTimeAsync(1)
    nav.querySelector('button')?.click()
    await flushPromises()
    await vi.advanceTimersByTimeAsync(1)
    app.find('button[aria-label="Next page"]').click()
    await flushPromises()
    expect(ipc).toHaveBeenLastCalledWith('dashboard_snapshot', { page: 1, selectedId: null })
    expect(nav.textContent).toContain('Fulfillment')
    app.unmount()
  })

})
