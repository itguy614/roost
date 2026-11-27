import { invoke } from '@tauri-apps/api/core'
import { defineStore } from 'pinia'
import { computed, ref, shallowRef } from 'vue'

export type Fault =
  | 'offline' | 'disconnected' | 'incompatible' | 'unsupported'
  | 'timeout' | 'access_denied' | 'invalid_response' | 'unavailable' | 'busy'

export interface Counts {
  desired: number
  running: number
  unresolved: number
}

export interface ProjectLink {
  id: string
  name: string
}

export interface ProjectRow extends ProjectLink {
  path: string
  desired_state: 'running' | 'stopped'
  observed:
    | { state: 'available'; summary: { workers: Counts; scheduler: Counts } }
    | { state: 'unsupported' | 'unavailable' }
}

export interface Dashboard {
  daemon: { pid: number; uptime_seconds: number; protocol_version: number }
  directory: ProjectLink[]
  projects: ProjectRow[]
  selected: ProjectRow | null
  page: number
  total: number
  pages: number
}

const faults: readonly Fault[] = [
  'offline', 'disconnected', 'incompatible', 'unsupported', 'timeout',
  'access_denied', 'invalid_response', 'unavailable', 'busy',
]

function faultOf(error: unknown): Fault {
  return typeof error === 'string' && faults.includes(error as Fault) ? error as Fault : 'unavailable'
}

export function discrepancy(counts: Counts) {
  if (counts.unresolved > 0) return 'Recovery blocked'
  if (counts.running < counts.desired) return 'Below desired'
  if (counts.running > counts.desired) return 'Above desired'
}

export const useDashboard = defineStore('dashboard', () => {
  const snapshot = shallowRef<Dashboard | null>(null)
  const fault = ref<Fault | null>(null)
  const receivedAt = ref<number | null>(null)
  const verifying = ref(true)
  const busy = ref(false)
  const requestedPage = ref(0)
  const selectedId = ref<string | null>(null)
  const stale = computed(() => snapshot.value !== null && (verifying.value || fault.value !== null))
  let active = false
  let revision = 0
  let retry = 2_000
  let timer: ReturnType<typeof setTimeout> | undefined

  function refresh() {
    if (!active || busy.value) return
    clearTimeout(timer)
    busy.value = true
    const scope = revision
    void invoke<Dashboard>('dashboard_snapshot', { page: requestedPage.value, selectedId: selectedId.value })
      .then((result) => {
        if (scope !== revision) return
        snapshot.value = result
        requestedPage.value = result.page
        receivedAt.value = Date.now()
        fault.value = null
        verifying.value = false
        retry = 2_000
      })
      .catch((error: unknown) => {
        if (scope !== revision) return
        fault.value = faultOf(error)
        verifying.value = false
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
    verifying.value = true
    clearTimeout(timer)
    if (next) {
      // A formerly offline or incompatible daemon could have started while hidden.
      fault.value = null
      retry = 2_000
      refresh()
    }
  }

  function selectPage(page: number) {
    if (page < 0 || page === requestedPage.value) return
    requestedPage.value = page
    revision += 1
    refresh()
  }

  function selectProject(id: string | null) {
    if (id === selectedId.value) return
    selectedId.value = id
    revision += 1
    refresh()
  }

  return { snapshot, fault, receivedAt, verifying, stale, busy, selectedId, refresh, selectPage, selectProject, setActive }
})
