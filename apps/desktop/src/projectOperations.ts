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
  | { kind: 'remove'; id: string; revision: number }
  | { kind: 'stop_workers' | 'stop_scheduler' | 'configure_scheduler'; id: string }
  | { kind: 'watch'; id: string; revision: number; enabled: boolean }
  | { kind: 'set_autostart'; id: string; revision: number; enabled: boolean }
  | { kind: 'create_profile'; id: string; name: string; connection: string; queues: string[]; instances: number }

export interface ProjectTarget { id: string; name: string; path: string }
interface Decision { action: ProjectAction; label: string; target: string; returnFocus?: HTMLElement }
interface Request extends Decision { key: string }
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

function operationTitle(decision: Decision) { return `${decision.label} — ${decision.target}` }

export function useProjectOperations(refresh: () => void, select: (id: string | null, removed?: string) => void) {
  const busy = ref(false)
  const replay = shallowRef<Request>()
  const confirmation = shallowRef<Decision & { message: string }>()
  const confirmationOrigin = shallowRef<HTMLElement>()
  const notice = shallowRef<{ title: string; text: string; error: boolean }>()
  const locked = computed(() => busy.value || !!replay.value || !!confirmation.value)

  async function send(request: Request) {
    busy.value = true
    notice.value = undefined
    try {
      const receipt = await invoke<Receipt>('project_action', { action: request.action, key: request.key })
      accepted(request, receipt)
    } catch (error) {
      const result = error as ActionError | null
      const action = request.action
      if (result?.kind === 'rejected') {
        if (result.reason === 'horizon' && 'allow_horizon' in action && !action.allow_horizon) {
          confirmationOrigin.value = request.returnFocus
          confirmation.value = {
            ...request,
            action: { ...action, allow_horizon: true },
            message: 'Horizon is listed in this project. Queue workers can compete with Horizon. Confirm only if you intend to run both.',
          }
        } else {
          notice.value = { title: operationTitle(request), text: rejected[result.reason] ?? 'The daemon rejected this operation. Project status has been refreshed.', error: true }
        }
      } else if (result?.kind === 'connection') {
        notice.value = { title: operationTitle(request), text: 'The operation could not connect to a compatible daemon. Check the connection and try again.', error: true }
      } else {
        replay.value = request
        notice.value = { title: operationTitle(request), text: 'The daemon reply was lost or inconclusive. Check the original receipt, or return to project status to reconcile the result.', error: true }
      }
    } finally {
      busy.value = false
      refresh()
    }
  }

  function accepted(request: Request, receipt: Receipt) {
    replay.value = undefined
    if (request.action.kind === 'add') select(receipt.project_id)
    if (request.action.kind === 'remove') select(null, request.action.id)
    notice.value = { title: operationTitle(request), text: 'Accepted by the daemon. Check observed state to confirm completion.', error: false }
  }

  async function add() {
    if (locked.value) return
    busy.value = true
    try {
      const path = await invoke<string | null>('choose_project_directory')
      if (path) await send({ action: { kind: 'add', path }, key: operationKey(), label: 'Register project', target: path })
    } catch {
      notice.value = { title: 'Register project', text: 'The project directory could not be selected.', error: true }
    } finally {
      busy.value = false
    }
  }

  async function discover(project: ProjectTarget, system: boolean) {
    if (locked.value) return
    busy.value = true
    try {
      return await invoke<{
        connections: Connections
        suggestions: { suggestions: { name: string }[] } | null
      }>('discover_project_queues', { id: project.id, provider: system ? 'system' : null })
    } catch {
      notice.value = { title: `Discover queues — ${project.path}`, text: 'Queue discovery could not read the project’s Laravel configuration. Check the PHP runtime and try again.', error: true }
    } finally {
      busy.value = false
      refresh()
    }
  }

  async function launch(kind: Launch['kind'], project: ProjectTarget, label: string, system: boolean, returnFocus?: HTMLElement) {
    if (locked.value) return
    busy.value = true
    const { id, path } = project
    try {
      const ticket = await invoke<string>('resolve_project_php', { id, provider: system ? 'system' : null })
      const action = { kind, id, ticket, allow_horizon: false } as const
      const payload: Launch = kind === 'start_workers' || kind === 'restart_workers'
        ? { ...action, kind }
        : { kind, id, ticket }
      await send({ action: payload, key: operationKey(), label, target: path, returnFocus })
    } catch {
      notice.value = { title: `${label} — ${path}`, text: 'PHP could not be resolved for this launch. Check the project provider or choose System PHP.', error: true }
    } finally {
      busy.value = false
    }
  }

  function act(action: ProjectAction, label: string, project: ProjectTarget, message?: string, returnFocus?: HTMLElement) {
    if (locked.value) return
    const decision = { action, label, target: project.path, returnFocus }
    if (message) {
      confirmationOrigin.value = returnFocus
      confirmation.value = { ...decision, message }
    } else void send({ ...decision, key: operationKey() })
  }

  function confirm() {
    const current = confirmation.value
    if (!current || busy.value) return
    confirmation.value = undefined
    void send({ ...current, key: operationKey() })
  }

  function cancel() {
    confirmation.value = undefined
  }

  async function reconcile(discard = false) {
    const current = replay.value
    if (!current || busy.value) return
    busy.value = true
    try {
      const receipt = await invoke<Receipt | null>('reconcile_project_action', { key: current.key, discard })
      if (receipt) accepted(current, receipt)
      else {
        replay.value = undefined
        notice.value = { title: operationTitle(current), text: discard
          ? 'Check the latest project status before deciding whether another action is needed.'
          : 'No committed receipt was found. Check the latest project status before deciding whether another action is needed.', error: true }
      }
    } catch {
      notice.value = { title: operationTitle(current), text: 'Receipt recovery is unavailable. Check again when connected, or return to project status.', error: true }
    } finally {
      busy.value = false
      refresh()
    }
  }

  return { busy, replay, confirmation, confirmationOrigin, notice, locked, add, discover, launch, act, confirm, cancel, reconcile }
}
