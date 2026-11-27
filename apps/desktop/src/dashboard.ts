import { invoke } from '@tauri-apps/api/core'
import { defineStore } from 'pinia'
import { ref, shallowRef } from 'vue'

export type Fault =
  | 'offline' | 'disconnected' | 'incompatible' | 'unsupported'
  | 'timeout' | 'access_denied' | 'invalid_response' | 'unavailable' | 'busy'

export interface Counts {
  desired: number
  running: number
  unresolved: number
}

export interface ProjectRow {
  id: string
  name: string
  path: string
  desired_state: 'running' | 'stopped'
  observed:
    | { state: 'available'; summary: { workers: Counts; scheduler: Counts } }
    | { state: 'unsupported' | 'unavailable' }
}

export interface Dashboard {
  daemon: { pid: number; uptime_seconds: number; protocol_version: number }
  projects: ProjectRow[]
  page: number
  total: number
  has_next: boolean
}

const faults: readonly Fault[] = [
  'offline', 'disconnected', 'incompatible', 'unsupported', 'timeout',
  'access_denied', 'invalid_response', 'unavailable', 'busy',
]

function faultOf(error: unknown): Fault {
  return typeof error === 'string' && faults.includes(error as Fault) ? error as Fault : 'unavailable'
}

export const useDashboard = defineStore('dashboard', () => {
  const snapshot = shallowRef<Dashboard | null>(null)
  const fault = ref<Fault | null>(null)
  const busy = ref(false)
  const requestedPage = ref(0)
  let active = false
  let revision = 0
  let retry = 2_000
  let timer: ReturnType<typeof setTimeout> | undefined

  function refresh() {
    if (!active || busy.value) return
    clearTimeout(timer)
    busy.value = true
    const scope = revision
    void invoke<Dashboard>('dashboard_snapshot', { page: requestedPage.value })
      .then((result) => {
        if (scope !== revision) return
        snapshot.value = result
        requestedPage.value = result.page
        fault.value = null
        retry = 2_000
      })
      .catch((error: unknown) => {
        if (scope === revision) fault.value = faultOf(error)
      })
      .finally(() => {
        busy.value = false
        if (!active) return
        if (scope !== revision) {
          refresh()
          return
        }
        timer = setTimeout(refresh, fault.value ? retry : 5_000)
        if (fault.value) retry = Math.min(retry * 2, 15_000)
      })
  }

  function setActive(next: boolean) {
    if (active === next) return
    active = next
    revision += 1
    clearTimeout(timer)
    if (next) refresh()
  }

  function selectPage(page: number) {
    if (page < 0 || page === requestedPage.value) return
    requestedPage.value = page
    revision += 1
    refresh()
  }

  return { snapshot, fault, busy, requestedPage, refresh, selectPage, setActive }
})
