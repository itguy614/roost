import { invoke } from '@tauri-apps/api/core'
import { beforeEach, expect, it, vi } from 'vitest'
import { useProjectOperations } from './projectOperations'

vi.mock('@tauri-apps/api/core', () => ({ invoke: vi.fn() }))
const ipc = vi.mocked(invoke)

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

it('replays the same resolved runtime ticket and exact mutation key after a lost reply', async () => {
  ipc.mockResolvedValueOnce('prepared-runtime')
    .mockRejectedValueOnce({ kind: 'uncertain' })
    .mockResolvedValueOnce({ project_id: 'project', operation_id: 'op' })
  const ops = useProjectOperations(vi.fn(), vi.fn())
  await ops.launch('restart_workers', 'project', 'Worker restart', true)
  expect(ops.replay.value).toBeDefined()
  expect(ops.locked.value).toBe(true)
  expect(ipc).toHaveBeenNthCalledWith(1, 'resolve_project_php', { id: 'project', provider: 'system' })
  expect(ipc.mock.calls[1][1]).toMatchObject({
    action: { kind: 'restart_workers', id: 'project', ticket: 'prepared-runtime', allow_horizon: false },
  })
  ops.act({ kind: 'stop_workers', id: 'project' }, 'Worker stop')
  expect(ipc).toHaveBeenCalledTimes(2)
  ops.retry()
  expect(ipc.mock.calls[2]).toEqual(ipc.mock.calls[1])
  await vi.waitFor(() => expect(ops.notice.value?.text).toContain('Worker restart accepted'))
  expect(ops.replay.value).toBeUndefined()
})

it('requires explicit Horizon confirmation and uses a new idempotency key for changed arguments', async () => {
  ipc.mockResolvedValueOnce('prepared-runtime')
    .mockRejectedValueOnce({ kind: 'rejected', reason: 'horizon' })
    .mockResolvedValueOnce({ project_id: 'project', operation_id: 'op' })
  const ops = useProjectOperations(vi.fn(), vi.fn())
  await ops.launch('start_workers', 'project', 'Worker start', false)
  expect(ipc).toHaveBeenCalledTimes(2)
  expect(ops.confirmation.value?.message).toContain('Horizon')
  ops.confirm()
  expect(ipc.mock.calls[2][1]).toMatchObject({ action: { allow_horizon: true } })
  expect((ipc.mock.calls[2][1] as { key: string }).key).not.toBe((ipc.mock.calls[1][1] as { key: string }).key)
  await vi.waitFor(() => expect(ops.notice.value?.text).toContain('Worker start accepted'))
})
