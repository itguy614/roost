import { invoke } from '@tauri-apps/api/core'
import { beforeEach, expect, it, vi } from 'vitest'
import { useProjectOperations } from './projectOperations'

vi.mock('@tauri-apps/api/core', () => ({ invoke: vi.fn() }))
const ipc = vi.mocked(invoke)
const project = { id: 'project', name: 'Dispatch', path: '/tmp/Dispatch' }

beforeEach(() => ipc.mockReset())

it('accepts registration only after a native choice and selects the returned UUID', async () => {
  ipc.mockResolvedValueOnce(null)
    .mockResolvedValueOnce('/tmp/TestApp')
    .mockResolvedValueOnce({ project_id: 'registered', operation_id: 'op' })
  const refresh = vi.fn()
  const select = vi.fn()
  const ops = useProjectOperations(refresh, select)
  await ops.add()
  expect(ipc).toHaveBeenCalledTimes(1)
  expect(select).not.toHaveBeenCalled()
  await ops.add()
  expect(ipc).toHaveBeenLastCalledWith('project_action', {
    action: { kind: 'add', path: '/tmp/TestApp' }, key: expect.stringMatching(/^[a-f\d-]{36}$/),
  })
  expect(select).toHaveBeenCalledWith('registered')
  expect(refresh).toHaveBeenCalledOnce()
})

it('looks up the original mutation key without resubmitting after a lost reply', async () => {
  ipc.mockResolvedValueOnce('prepared-runtime')
    .mockRejectedValueOnce({ kind: 'uncertain' })
    .mockResolvedValueOnce({ project_id: 'project', operation_id: 'op' })
  const ops = useProjectOperations(vi.fn(), vi.fn())
  await ops.launch('restart_workers', project, 'Worker restart', true)
  expect(ops.replay.value).toBeDefined()
  expect(ops.locked.value).toBe(true)
  expect(ipc).toHaveBeenNthCalledWith(1, 'resolve_project_php', { id: 'project', provider: 'system' })
  expect(ipc.mock.calls[1][1]).toMatchObject({
    action: { kind: 'restart_workers', id: 'project', ticket: 'prepared-runtime', allow_horizon: false },
  })
  ops.act({ kind: 'stop_workers', id: 'project' }, 'Worker stop', project)
  expect(ipc).toHaveBeenCalledTimes(2)
  const original = ipc.mock.calls[1][1] as { key: string }
  await ops.reconcile()
  expect(ipc.mock.calls[2]).toEqual(['reconcile_project_action', { key: original.key, discard: false }])
  expect(ops.notice.value?.title).toContain('/tmp/Dispatch')
  expect(ops.notice.value?.text).toContain('Accepted by the daemon')
  expect(ops.replay.value).toBeUndefined()
})

it('requires explicit Horizon confirmation and uses a new idempotency key for changed arguments', async () => {
  ipc.mockResolvedValueOnce('prepared-runtime')
    .mockRejectedValueOnce({ kind: 'rejected', reason: 'horizon' })
    .mockResolvedValueOnce({ project_id: 'project', operation_id: 'op' })
  const ops = useProjectOperations(vi.fn(), vi.fn())
  await ops.launch('start_workers', project, 'Worker start', false)
  expect(ipc).toHaveBeenCalledTimes(2)
  expect(ops.confirmation.value?.message).toContain('Horizon')
  ops.confirm()
  expect(ipc.mock.calls[2][1]).toMatchObject({ action: { allow_horizon: true } })
  expect((ipc.mock.calls[2][1] as { key: string }).key).not.toBe((ipc.mock.calls[1][1] as { key: string }).key)
  await vi.waitFor(() => expect(ops.notice.value?.text).toContain('Accepted by the daemon'))
})


it('allows leaving a disconnected receipt lookup without submitting a replacement', async () => {
  ipc.mockRejectedValueOnce({ kind: 'uncertain' })
    .mockRejectedValueOnce({ kind: 'connection', fault: 'offline' })
    .mockResolvedValueOnce(null)
  const ops = useProjectOperations(vi.fn(), vi.fn())
  ops.act({ kind: 'stop_workers', id: project.id }, 'Stop workers', project)
  await vi.waitFor(() => expect(ops.replay.value).toBeDefined())
  const key = ops.replay.value?.key
  await ops.reconcile()
  expect(ops.replay.value?.key).toBe(key)
  await ops.reconcile(true)
  expect(ops.locked.value).toBe(false)
  expect(ipc.mock.calls.map(([command]) => command)).toEqual([
    'project_action', 'reconcile_project_action', 'reconcile_project_action',
  ])
  expect(ipc).toHaveBeenLastCalledWith('reconcile_project_action', { key, discard: true })
})
