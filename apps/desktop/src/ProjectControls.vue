<script setup lang="ts">
import { CuiButton, CuiInput } from '@itguy614/clean-ui'
import { computed, ref } from 'vue'
import type { ProjectRow } from './dashboard'
import type { ProjectDetail, useProjectOperations } from './projectOperations'

const props = defineProps<{
  project: ProjectRow
  detail: ProjectDetail
  locked: boolean
  operations: ReturnType<typeof useProjectOperations>
}>()
const system = ref(false)
const profileName = ref('')
const connection = ref('')
const queues = ref('')
const instances = ref('1')
const slots = computed(() => props.detail.workers)
const blocked = computed(() => props.project.observed.state === 'available' &&
  props.project.observed.summary.workers.unresolved > 0 || slots.value?.some((slot) => slot.recovery_blocked))
const schedulerBlocked = computed(() => props.project.observed.state === 'available' &&
  props.project.observed.summary.scheduler.unresolved > 0 || props.detail.scheduler?.process?.recovery_blocked)
const active = computed(() => slots.value?.some((slot) => !['stopped', 'failed'].includes(slot.state)))
const workerIntent = computed(() => active.value || (props.project.desired_state === 'running' && !!slots.value?.length))
const schedulerIntent = computed(() => !!props.detail.scheduler?.process &&
  (props.detail.scheduler.desired_state === 'running' || props.detail.scheduler.process.state !== 'stopped'))
const supports = (method: string) => props.detail.capabilities.includes(method)
const canLaunch = computed(() => supports('provider.resolvePhp'))
const editable = computed(() => props.detail.config_owner === 'local')
const count = computed(() => Number(instances.value))
const profileValid = computed(() => profileName.value.trim() && connection.value.trim() &&
  Number.isInteger(count.value) && count.value >= 1 && count.value <= 32)

const discovered = ref<Awaited<ReturnType<typeof props.operations.discover>>>()
async function discoverQueues() {
  const value = await props.operations.discover(props.project.id, system.value)
  discovered.value = value
}

function createProfile() {
  if (!profileValid.value || props.locked) return
  props.operations.act({
    kind: 'create_profile', id: props.project.id, name: profileName.value.trim(), connection: connection.value.trim(),
    queues: queues.value.split(',').map((queue) => queue.trim()).filter(Boolean), instances: count.value,
  }, 'Worker profile creation')
}
</script>

<template>
  <section class="project-controls" aria-label="Project operations">
    <div class="operation-actions">
      <CuiButton v-if="workerIntent && supports('project.stop')" size="sm" variant="solid" :disabled="locked"
        @click="operations.act({ kind: 'stop_workers', id: project.id }, 'Worker stop')">Stop workers</CuiButton>
      <CuiButton v-else-if="!workerIntent && canLaunch && supports('project.start')" size="sm" variant="solid"
        :disabled="locked || !!blocked || slots === null || !detail.profiles?.some((p) => p.enabled)"
        @click="operations.launch('start_workers', project.id, 'Worker start', system)">Start workers</CuiButton>
      <CuiButton v-if="workerIntent && canLaunch && supports('project.restart')" size="sm" variant="ghost"
        :disabled="locked || !!blocked || !detail.profiles?.some((p) => p.enabled)"
        @click="operations.launch('restart_workers', project.id, 'Worker restart', system)">Restart workers</CuiButton>
    </div>
    <p v-if="blocked" class="attention">Worker launch is blocked pending process cleanup.</p>
    <p v-if="detail.profiles && !detail.profiles.some((p) => p.enabled)" class="subtitle">Create an enabled worker profile before starting workers.</p>
    <p v-if="slots === null" class="subtitle">Worker observations are unavailable.</p>
    <ul v-else-if="slots.length" class="slot-list" aria-label="Observed worker states">
      <li v-for="slot in slots" :key="slot.id">{{ slot.recovery_blocked ? 'Recovery blocked' : slot.state }}</li>
    </ul>

    <section>
      <h2>PHP runtime</h2>
      <p>Configured provider: {{ detail.provider === 'herd' ? 'Laravel Herd' : 'System PHP' }}</p>
      <label v-if="canLaunch"><input v-model="system" type="checkbox" :disabled="locked"> Use System PHP for this launch</label>
    </section>

    <section v-if="detail.profiles">
      <h2>Worker profiles</h2>
      <CuiButton v-if="supports('queue.discover')" size="sm" :disabled="locked" @click="discoverQueues">
        Discover queue connections
      </CuiButton>
      <div v-if="detail.connections" class="discovery-result">
        <p>Laravel connections: {{ detail.connections.connections.map((item) => item.name).join(', ') || 'None returned' }}.</p>
        <p v-if="detail.connections.default">Default connection: {{ detail.connections.default }}.</p>
        <p v-if="discovered?.suggestions && discovered.connections.observed_at === detail.connections.observed_at">
          Queue-name suggestions are incomplete: {{ discovered.suggestions.suggestions.map((item) => item.name).join(', ') || 'None found' }}.
        </p>
      </div>
      <ul class="profile-list">
        <li v-for="profile in detail.profiles" :key="profile.id">
          <strong>{{ profile.name }}</strong> · {{ profile.connection }} · {{ profile.desired_instances }} instances
          <small>{{ profile.queues.length ? profile.queues.join(', ') : 'Connection default queue' }}</small>
        </li>
      </ul>
      <form v-if="editable && supports('worker.profile.create')" class="profile-form" @submit.prevent="createProfile">
        <label>Profile name<CuiInput v-model="profileName" size="sm" :disabled="locked" required /></label>
        <label>Queue connection<CuiInput v-model="connection" size="sm" :disabled="locked" required /></label>
        <label>Queues in priority order<CuiInput v-model="queues" size="sm" :disabled="locked" placeholder="critical, default" /></label>
        <label>Instances<CuiInput v-model="instances" size="sm" :disabled="locked" inputmode="numeric" /></label>
        <CuiButton type="submit" size="sm" :disabled="locked || !profileValid">Create profile</CuiButton>
      </form>
    </section>

    <section v-if="detail.scheduler">
      <h2>Scheduler</h2>
      <p v-if="detail.scheduler.configured">
        {{ detail.scheduler.desired_state === 'running' ? 'Running requested' : 'Stopped requested' }}
        · {{ detail.scheduler.process?.state ?? 'No observed process' }}
      </p>
      <p v-else>Scheduler is unconfigured.</p>
      <p v-if="schedulerBlocked" class="attention">Scheduler launch is blocked pending process cleanup.</p>
      <div class="operation-actions">
        <CuiButton v-if="!detail.scheduler.configured && editable && supports('scheduler.configure')" size="sm" :disabled="locked"
          @click="operations.act({ kind: 'configure_scheduler', id: project.id }, 'Scheduler configuration')">Configure scheduler</CuiButton>
        <CuiButton v-if="detail.scheduler.enabled && !schedulerIntent && canLaunch && supports('scheduler.start')" size="sm"
          :disabled="locked || !!schedulerBlocked" @click="operations.launch('start_scheduler', project.id, 'Scheduler start', system)">Start scheduler</CuiButton>
        <CuiButton v-if="schedulerIntent && supports('scheduler.stop')" size="sm" :disabled="locked"
          @click="operations.act({ kind: 'stop_scheduler', id: project.id }, 'Scheduler stop')">Stop scheduler</CuiButton>
        <CuiButton v-if="schedulerIntent && canLaunch && supports('scheduler.restart')" size="sm" :disabled="locked || !!schedulerBlocked"
          @click="operations.launch('restart_scheduler', project.id, 'Scheduler restart', system)">Restart scheduler</CuiButton>
      </div>
    </section>

    <section v-if="detail.watch">
      <h2>Watching</h2>
      <p class="capitalize">{{ detail.watch.state }}<span v-if="detail.watch.pending"> · Change batch pending</span></p>
      <CuiButton v-if="editable && supports(detail.watch_enabled ? 'watch.disable' : 'watch.enable')"
        size="sm" :disabled="locked"
        @click="operations.act({ kind: 'watch', id: project.id, revision: detail.revision, enabled: !detail.watch_enabled }, 'Watch setting')">
        {{ detail.watch_enabled ? 'Disable watch' : 'Enable watch' }}
      </CuiButton>
      <ul class="profile-list">
        <li v-for="watch in detail.watch.shared ?? []" :key="watch.group_id + watch.path">{{ watch.path }} · {{ watch.state }}</li>
      </ul>
    </section>

    <section>
      <h2>Project settings</h2>
      <p v-if="!editable">Configuration is owned by a portable file.</p>
      <p>Daemon autostart for this project is {{ detail.autostart ? 'enabled' : 'disabled' }}.</p>
      <div class="operation-actions">
        <CuiButton v-if="editable && supports('project.setAutostart')" size="sm" :disabled="locked"
          @click="operations.act({ kind: 'set_autostart', id: project.id, enabled: !detail.autostart }, 'Autostart setting')">
          {{ detail.autostart ? 'Disable autostart' : 'Enable autostart' }}
        </CuiButton>
        <CuiButton v-if="supports('project.remove')" size="sm" color="error" :disabled="locked || !!blocked || !!schedulerBlocked || !!active || schedulerIntent"
          @click="operations.act({ kind: 'remove', id: project.id }, 'Project removal', 'Remove this registration and its saved Roost settings? Laravel files stay on disk.')">
          Remove project
        </CuiButton>
      </div>
    </section>
  </section>
</template>
