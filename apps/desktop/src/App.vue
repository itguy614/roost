<script setup lang="ts">
import {
  CuiAlert, CuiButton, CuiEmptyState, CuiList, CuiListItem, CuiPagination,
  CuiSidebar, CuiSidebarContent, CuiSidebarFooter, CuiSidebarHeader, CuiStatusDot,
  CuiTable, CuiTableBody, CuiTableCell, CuiTableHead, CuiTableRow, CuiTooltip,
} from '@itguy614/clean-ui'
import { listen } from '@tauri-apps/api/event'
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue'
import { discrepancy, useDashboard, type Fault } from './dashboard'
import icon from '../src-tauri/icons/icon.png'

const dashboard = useDashboard()
const heading = ref<HTMLElement | null>(null)
const snapshot = computed(() => dashboard.snapshot)
const activeProject = computed(() => {
  const id = dashboard.selectedId
  if (snapshot.value?.selected?.id === id) return snapshot.value.selected
  return snapshot.value?.projects.find((project) => project.id === id)
})
const selectedTitle = computed(() => activeProject.value?.name ??
  snapshot.value?.directory.find((project) => project.id === dashboard.selectedId)?.name ??
  (dashboard.selectedId ? 'Project' : 'Dashboard'))
const problems: Record<Fault, { title: string; description: string }> = {
  offline: { title: 'Roost daemon is offline', description: 'Start the daemon, then retry the connection.' },
  disconnected: { title: 'Connection lost', description: 'Roost will try to reconnect automatically.' },
  incompatible: { title: 'Incompatible daemon version', description: 'Run a daemon that supports Roost IPC v1, then retry.' },
  unsupported: { title: 'Dashboard unavailable', description: 'This daemon does not provide the dashboard’s required reads.' },
  timeout: { title: 'Daemon response timed out', description: 'Roost will try this read again.' },
  access_denied: { title: 'Cannot access the daemon socket', description: 'Check that both apps use the same private state directory.' },
  invalid_response: { title: 'Unexpected daemon response', description: 'Check the daemon version and try again.' },
  unavailable: { title: 'Dashboard temporarily unavailable', description: 'Retry to refresh project status.' },
  busy: { title: 'Dashboard is busy', description: 'Roost will try this read again.' },
}
const problem = computed(() => dashboard.fault && problems[dashboard.fault])
const status = computed(() => {
  if (dashboard.verifying) return 'Checking connection'
  if (dashboard.fault === 'offline') return 'Daemon offline'
  if (problem.value) return 'Daemon unavailable'
  return 'Daemon connected'
})
const receivedTime = computed(() => dashboard.receivedAt === null ? null :
  new Date(dashboard.receivedAt).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit', second: '2-digit' }))
const services = ['workers', 'scheduler'] as const

let nativeVisible = true
let mounted = false
let unlisten: (() => void) | undefined

function syncVisibility() {
  dashboard.setActive(nativeVisible && !document.hidden)
}

onMounted(() => {
  mounted = true
  document.addEventListener('visibilitychange', syncVisibility)
  syncVisibility()
  void listen<boolean>('roost://window-visible', ({ payload }) => {
    nativeVisible = payload
    syncVisibility()
  }).then((stop) => {
    if (mounted) unlisten = stop
    else stop()
  }).catch(() => {
    // Document visibility remains available in a browser.
  })
})

onUnmounted(() => {
  mounted = false
  document.removeEventListener('visibilitychange', syncVisibility)
  unlisten?.()
  dashboard.setActive(false)
})

function navigate(id: string | null) {
  dashboard.selectProject(id)
  void nextTick(() => heading.value?.focus())
}

function selectPage(page: number) {
  if (!dashboard.busy && !problem.value && !dashboard.verifying) dashboard.selectPage(page - 1)
}
</script>

<template>
  <div class="shell antialiased">
    <CuiSidebar label="Roost sidebar" collapse-below="never">
      <CuiSidebarHeader title="Roost">
        <template #icon><img :src="icon" alt="" width="25" height="25"></template>
      </CuiSidebarHeader>
      <CuiSidebarContent>
        <CuiList>
          <CuiListItem title="Dashboard" :aria-current="dashboard.selectedId === null ? 'page' : undefined" @click="navigate(null)" />
        </CuiList>
        <template v-if="snapshot?.directory.length">
          <p class="nav-label" id="project-nav">Projects</p>
          <CuiList aria-labelledby="project-nav">
            <CuiListItem
              v-for="project in snapshot.directory"
              :key="project.id"
              :title="project.name"
              :aria-current="dashboard.selectedId === project.id ? 'page' : undefined"
              @click="navigate(project.id)"
            />
          </CuiList>
        </template>
      </CuiSidebarContent>
      <CuiSidebarFooter>
        <p class="sidebar-status" role="status">
          <CuiStatusDot :tone="dashboard.verifying ? 'info' : problem ? 'neutral' : 'success'" />
          {{ status }}
        </p>
      </CuiSidebarFooter>
    </CuiSidebar>

    <main>
      <header class="page-header">
        <div class="min-w-0">
          <h1 ref="heading" tabindex="-1">{{ selectedTitle }}</h1>
          <p v-if="activeProject" class="subtitle truncate" :title="activeProject.path">{{ activeProject.path }}</p>
          <p v-else-if="snapshot && !dashboard.selectedId" class="subtitle">{{ snapshot.total }} {{ snapshot.total === 1 ? 'project' : 'projects' }}</p>
          <p v-if="receivedTime" class="freshness" :class="{ stale: dashboard.stale }">
            {{ dashboard.stale ? 'Stale snapshot from' : 'Last updated at' }} {{ receivedTime }}
          </p>
        </div>
        <CuiTooltip v-if="snapshot && !problem && !dashboard.verifying" text="Read current state from the daemon" placement="bottom">
          <CuiButton size="sm" color="surface-dark" :disabled="dashboard.busy" focusable-when-disabled @click="dashboard.refresh()">
            Refresh
          </CuiButton>
        </CuiTooltip>
      </header>

      <CuiAlert v-if="problem" class="view-notice" :title="problem.title" color="warning" entrance="none" live="polite">
        <p>{{ problem.description }}</p>
        <p v-if="snapshot">Showing the last successful project snapshot.</p>
        <template #actions>
          <CuiButton size="sm" variant="solid" :disabled="dashboard.busy" focusable-when-disabled @click="dashboard.refresh()">
            Retry connection
          </CuiButton>
        </template>
      </CuiAlert>
      <CuiAlert v-else-if="dashboard.verifying" class="view-notice" title="Checking daemon connection" entrance="none" live="polite">
        <span v-if="snapshot">Showing the previous snapshot until a new read succeeds.</span>
      </CuiAlert>

      <section v-if="dashboard.selectedId" class="project-detail" aria-label="Project status">
        <dl v-if="activeProject">
          <div><dt>Worker intent</dt><dd>{{ activeProject.desired_state === 'running' ? 'Running requested' : 'Stopped requested' }}</dd></div>
          <template v-if="activeProject.observed.state === 'available'">
            <div v-for="service in services" :key="service">
              <dt><span class="capitalize">{{ service }}</span> running / desired</dt>
              <dd>
                {{ activeProject.observed.summary[service].running }} / {{ activeProject.observed.summary[service].desired }}
                <span v-if="discrepancy(activeProject.observed.summary[service])" class="attention">
                  {{ discrepancy(activeProject.observed.summary[service]) }}
                </span>
              </dd>
            </div>
            <div v-if="activeProject.desired_state === 'running' && activeProject.observed.summary.workers.desired === 0">
              <dt>Worker capacity</dt><dd>No worker instances requested</dd>
            </div>
          </template>
          <div v-else><dt>Observed status</dt><dd class="capitalize">{{ activeProject.observed.state }}</dd></div>
        </dl>
        <p v-else-if="dashboard.busy" class="subtitle" role="status">Reading project status…</p>
        <CuiEmptyState
          v-else-if="!problem && !dashboard.verifying" size="sm" title-as="h2"
          title="Project no longer registered" description="Choose another project from the sidebar."
        />
      </section>

      <section v-else-if="snapshot?.total" aria-label="Registered projects">
        <CuiTable class="dashboard-table" size="sm" hoverable fixed-layout min-width="28rem">
          <caption class="sr-only">Registered projects and their latest observed processes</caption>
          <CuiTableHead>
            <CuiTableRow>
              <CuiTableCell width="37%" scope="col">Project</CuiTableCell>
              <CuiTableCell width="23%" scope="col">Worker intent</CuiTableCell>
              <CuiTableCell width="20%" scope="col">Workers <small>running / desired</small></CuiTableCell>
              <CuiTableCell width="20%" scope="col">Scheduler <small>running / desired</small></CuiTableCell>
            </CuiTableRow>
          </CuiTableHead>
          <CuiTableBody>
            <CuiTableRow v-for="project in snapshot.projects" :key="project.id">
              <CuiTableCell header scope="row">
                <CuiButton class="project-link" size="sm" variant="ghost" @click="navigate(project.id)">{{ project.name }}</CuiButton>
                <span class="path truncate" :title="project.path">{{ project.path }}</span>
              </CuiTableCell>
              <CuiTableCell>{{ project.desired_state === 'running' ? 'Running requested' : 'Stopped requested' }}
                <small v-if="project.desired_state === 'running' && project.observed.state === 'available' && project.observed.summary.workers.desired === 0">
                  No instances requested
                </small>
              </CuiTableCell>
              <template v-if="project.observed.state === 'available'">
                <CuiTableCell v-for="service in services" :key="service">
                  {{ project.observed.summary[service].running }} / {{ project.observed.summary[service].desired }}
                  <span v-if="discrepancy(project.observed.summary[service])" class="attention">
                    {{ discrepancy(project.observed.summary[service]) }}
                  </span>
                </CuiTableCell>
              </template>
              <CuiTableCell v-else :colspan="2" class="subtitle capitalize">{{ project.observed.state }}</CuiTableCell>
            </CuiTableRow>
          </CuiTableBody>
        </CuiTable>
        <CuiPagination v-if="snapshot.pages > 1" class="pager" size="sm" hide-per-page hide-info
          :current-page="snapshot.page + 1" :total-pages="snapshot.pages" @update:current-page="selectPage"
        />
      </section>
      <CuiEmptyState v-else-if="snapshot && !problem && !dashboard.verifying" size="sm" title-as="h2"
        title="No projects yet" description="Use roost project add PATH to register a Laravel project."
      />
    </main>
  </div>
</template>
