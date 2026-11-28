import { invoke } from '@tauri-apps/api/core'
import { computed, ref, shallowRef } from 'vue'

export interface Slot {
  id: string
  state: 'stopped' | 'starting' | 'running' | 'stopping' | 'backoff' | 'failed'
  recovery_blocked: boolean
}

interface Connections {
  default: string | null
  connections: { name: string; driver: string | null }[]
  observed_at: number
}

export interface ProjectDetail {
  watch_enabled: boolean
  connections: Connections | null
  revision: number
  provider: 'herd' | 'system'
  autostart: boolean
  config_owner: 'local' | 'file'
  profiles: { id: string; name: string; connection: string; queues: string[]; desired_instances: number; enabled: boolean }[] | null
  workers: Slot[] | null
  scheduler: { configured: boolean; enabled: boolean; desired_state: 'running' | 'stopped'; process: Slot | null } | null
  watch: { state: string; pending: boolean; shared?: { group_id: string; path: string; state: string }[] } | null
  capabilities: string[]
}

type WorkerLaunch = { kind: 'start_workers' | 'restart_workers'; id: string; ticket: string; allow_horizon: boolean }
type Launch = WorkerLaunch | { kind: 'start_scheduler' | 'restart_scheduler'; id: string; ticket: string }
export type ProjectAction = Launch
  | { kind: 'add'; path: string }
  | { kind: 'remove' | 'stop_workers' | 'stop_scheduler' | 'configure_scheduler'; id: string }
  | { kind: 'watch'; id: string; revision: number; enabled: boolean }
  | { kind: 'set_autostart'; id: string; enabled: boolean }
  | { kind: 'create_profile'; id: string; name: string; connection: string; queues: string[]; instances: number }

interface Request { action: ProjectAction; key: string; label: string }
interface Receipt { project_id: string; operation_id: string }
type ActionError =
  | { kind: 'uncertain' }
  | { kind: 'rejected'; reason: string }
  | { kind: 'connection'; fault: string }

function operationKey() {
  // getRandomValues is also available on WebViews whose local asset scheme
  // does not expose the secure-context-only randomUUID method.
  const bytes = crypto.getRandomValues(new Uint8Array(16))
  bytes[6] = (bytes[6] & 15) | 64
  bytes[8] = (bytes[8] & 63) | 128
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')
  return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20)].join('-')
}

const rejected: Record<string, string> = {
  conflict: 'The daemon rejected the request because it conflicts with current project configuration or process ownership.',
  validation: 'The daemon rejected the supplied settings. For registration, choose a Laravel directory that contains artisan.',
  not_found: 'The project is no longer registered.',
  expired_launch: 'The prepared PHP runtime is unavailable. Resolve it again before launching.',
}

export function useProjectOperations(refresh: () => void, select: (id: string | null) => void) {
  const busy = ref(false)
  const replay = shallowRef<Request>()
  const confirmation = shallowRef<{ action: ProjectAction; label: string; message: string }>()
  const notice = shallowRef<{ text: string; error: boolean }>()
  const locked = computed(() => busy.value || !!replay.value || !!confirmation.value)

  async function send(request: Request) {
    busy.value = true
    notice.value = undefined
    try {
      const receipt = await invoke<Receipt>('project_action', { action: request.action, key: request.key })
      replay.value = undefined
      if (request.action.kind === 'add') select(receipt.project_id)
      if (request.action.kind === 'remove') select(null)
      notice.value = { text: `${request.label} accepted. The project view refreshes from daemon observations.`, error: false }
    } catch (error) {
      const result = error as ActionError | null
      const action = request.action
      if (result?.kind === 'rejected') {
        replay.value = undefined
        if (result.reason === 'horizon' && 'allow_horizon' in action && !action.allow_horizon) {
          confirmation.value = {
            action: { ...action, allow_horizon: true },
            label: request.label,
            message: 'Horizon is listed in this project. Queue workers can compete with Horizon. Confirm only if you intend to run both.',
          }
        } else {
          notice.value = { text: rejected[result.reason] ?? 'The daemon rejected this operation.', error: true }
        }
      } else if (result?.kind === 'connection' && !replay.value) {
        notice.value = { text: 'The operation could not connect to a compatible daemon. Check the connection and try again.', error: true }
      } else {
        replay.value = request
        notice.value = { text: 'The daemon reply was lost or inconclusive. Retry the same request to recover its acknowledgement.', error: true }
      }
    } finally {
      busy.value = false
      refresh()
    }
  }

  async function add() {
    if (locked.value) return
    busy.value = true
    try {
      const path = await invoke<string | null>('choose_project_directory')
      if (path) await send({ action: { kind: 'add', path }, key: operationKey(), label: 'Project registration' })
    } catch {
      notice.value = { text: 'The project directory could not be selected.', error: true }
    } finally {
      busy.value = false
    }
  }

  async function discover(id: string, system: boolean) {
    if (locked.value) return
    busy.value = true
    try {
      return await invoke<{
        connections: Connections
        suggestions: { suggestions: { name: string }[] } | null
      }>('discover_project_queues', { id, provider: system ? 'system' : null })
    } catch {
      notice.value = { text: 'Queue discovery could not read the project’s Laravel configuration. Check the PHP runtime and try again.', error: true }
    } finally {
      busy.value = false
      refresh()
    }
  }

  async function launch(kind: Launch['kind'], id: string, label: string, system: boolean) {
    if (locked.value) return
    busy.value = true
    try {
      const ticket = await invoke<string>('resolve_project_php', { id, provider: system ? 'system' : null })
      const action = { kind, id, ticket, allow_horizon: false } as const
      const payload: Launch = kind === 'start_workers' || kind === 'restart_workers'
        ? { ...action, kind }
        : { kind, id, ticket }
      await send({ action: payload, key: operationKey(), label })
    } catch {
      notice.value = { text: 'PHP could not be resolved for this launch. Check the project provider or choose System PHP.', error: true }
    } finally {
      busy.value = false
    }
  }

  function act(action: ProjectAction, label: string, message?: string) {
    if (locked.value) return
    if (message) confirmation.value = { action, label, message }
    else void send({ action, label, key: operationKey() })
  }

  function confirm() {
    const current = confirmation.value
    if (!current || busy.value) return
    confirmation.value = undefined
    void send({ action: current.action, label: current.label, key: operationKey() })
  }

  function retry() {
    if (replay.value && !busy.value) void send(replay.value)
  }

  return { busy, replay, confirmation, notice, locked, add, discover, launch, act, confirm, retry }
}
