<script setup lang="ts">
import { listen } from '@tauri-apps/api/event'
import { computed, nextTick, onMounted, onUnmounted, ref } from 'vue'
import { useDashboard, type Fault, type ProjectRow } from './dashboard'
import icon from '../src-tauri/icons/icon.png'

const dashboard = useDashboard()
const activeProjectId = ref<string | null>(null)
const heading = ref<HTMLElement | null>(null)
const snapshot = computed(() => dashboard.snapshot)
const activeProject = computed(() =>
  snapshot.value?.projects.find((project) => project.id === activeProjectId.value),
)
const selectedTitle = computed(() => activeProject.value?.name ?? 'Dashboard')
const problemCount = computed(() => snapshot.value?.projects.filter(needsAttention).length ?? 0)
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
    // The document visibility listener also covers browser-only development.
  })
})

onUnmounted(() => {
  mounted = false
  document.removeEventListener('visibilitychange', syncVisibility)
  unlisten?.()
  dashboard.setActive(false)
})

function needsAttention(project: ProjectRow) {
  return project.observed.state === 'available' &&
    (project.observed.summary.workers.unresolved > 0 || project.observed.summary.scheduler.unresolved > 0)
}

function navigate(id: string | null) {
  activeProjectId.value = id
  void nextTick(() => heading.value?.focus())
}

function selectPage(page: number) {
  navigate(null)
  dashboard.selectPage(page)
}
</script>

<template>
  <div class="shell antialiased">
    <aside class="sidebar">
      <div class="brand"><img :src="icon" alt="" width="25" height="25"> Roost</div>
      <nav aria-label="Sidebar">
        <button
          class="nav-item"
          :aria-current="!activeProject ? 'page' : undefined"
          @click="navigate(null)"
        >
          <svg width="17" height="17" viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <rect x="3.5" y="4.5" width="17" height="15" rx="2" />
            <path d="M3.5 10h17M9 10v9.5" />
          </svg>
          Dashboard
        </button>
        <template v-if="snapshot?.projects.length">
          <p class="nav-label">Projects</p>
          <button
            v-for="project in snapshot.projects"
            :key="project.id"
            class="nav-item"
            :aria-current="activeProject?.id === project.id ? 'page' : undefined"
            @click="navigate(project.id)"
          >
            <svg width="17" height="17" viewBox="0 0 24 24" fill="none" aria-hidden="true">
              <path d="M3.5 7a2 2 0 0 1 2-2h4l2 2h7a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2h-13a2 2 0 0 1-2-2z" />
            </svg>
            <span class="truncate">{{ project.name }}</span>
          </button>
        </template>
      </nav>
      <p class="sidebar-status" role="status">
        <span class="dot" :class="{ online: snapshot && !problem }" aria-hidden="true" />
        {{ problem ? (dashboard.fault === 'offline' ? 'Daemon offline' : 'Daemon unavailable') : snapshot ? 'Daemon connected' : 'Connecting' }}
      </p>
    </aside>

    <main>
      <header class="page-header">
        <div class="min-w-0">
          <h1 ref="heading" tabindex="-1">{{ selectedTitle }}</h1>
          <p v-if="activeProject" class="subtitle truncate" :title="activeProject.path">{{ activeProject.path }}</p>
          <p v-else-if="snapshot" class="subtitle">{{ snapshot.total }} {{ snapshot.total === 1 ? 'project' : 'projects' }}</p>
        </div>
        <button v-if="snapshot && !problem" class="button" :aria-disabled="dashboard.busy" @click="dashboard.refresh()">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <path d="M20 6v5h-5M20 11a8 8 0 1 0-2 7" />
          </svg>
          Refresh
        </button>
      </header>

      <section v-if="problem" class="notice" role="status">
        <h2>{{ problem.title }}</h2>
        <p>{{ problem.description }}</p>
        <p v-if="snapshot">Showing the last successful project snapshot.</p>
        <button class="button primary" :aria-disabled="dashboard.busy" @click="dashboard.refresh()">
          Retry connection
        </button>
      </section>
      <p v-else-if="!snapshot" class="message" role="status">Connecting to the Roost daemon…</p>

      <section v-if="snapshot && activeProject" class="project-detail" aria-label="Project status">
        <dl>
          <div><dt>Worker intent</dt><dd class="capitalize">{{ activeProject.desired_state }}</dd></div>
          <template v-if="activeProject.observed.state === 'available'">
            <div><dt>Workers running / desired</dt><dd>{{ activeProject.observed.summary.workers.running }} / {{ activeProject.observed.summary.workers.desired }}</dd></div>
            <div><dt>Scheduler running / desired</dt><dd>{{ activeProject.observed.summary.scheduler.running }} / {{ activeProject.observed.summary.scheduler.desired }}</dd></div>
          </template>
          <div v-else><dt>Observed status</dt><dd class="capitalize">{{ activeProject.observed.state }}</dd></div>
        </dl>
        <p v-if="needsAttention(activeProject)" class="attention">Recovery blocked pending process cleanup.</p>
      </section>

      <template v-else-if="snapshot">
        <section v-if="snapshot.total" aria-label="Registered projects">
          <p v-if="problemCount && !problem" class="attention">
            Recovery blocked in {{ problemCount }} {{ problemCount === 1 ? 'project on this page' : 'projects on this page' }}.
          </p>
          <div class="table-scroll">
            <table>
              <caption class="sr-only">Registered projects and their latest observed processes</caption>
              <thead>
                <tr>
                  <th scope="col">Project</th>
                  <th scope="col">Worker intent</th>
                  <th scope="col">Workers <span>running / desired</span></th>
                  <th scope="col">Scheduler <span>running / desired</span></th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="project in snapshot.projects" :key="project.id">
                  <th scope="row">
                    <button class="project-link" @click="navigate(project.id)">{{ project.name }}</button>
                    <span class="path truncate" :title="project.path">{{ project.path }}</span>
                  </th>
                  <td class="capitalize">{{ project.desired_state }}</td>
                  <template v-if="project.observed.state === 'available'">
                    <td>
                      {{ project.observed.summary.workers.running }} / {{ project.observed.summary.workers.desired }}
                      <span v-if="project.observed.summary.workers.unresolved" class="blocked">Recovery blocked</span>
                    </td>
                    <td>
                      {{ project.observed.summary.scheduler.running }} / {{ project.observed.summary.scheduler.desired }}
                      <span v-if="project.observed.summary.scheduler.unresolved" class="blocked">Recovery blocked</span>
                    </td>
                  </template>
                  <td v-else colspan="2" class="muted capitalize">{{ project.observed.state }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <nav v-if="snapshot.total > snapshot.projects.length" class="pager" aria-label="Project pages">
            <button class="button" :disabled="snapshot.page === 0 || dashboard.busy || !!problem" @click="selectPage(snapshot.page - 1)">Previous</button>
            <span>Page {{ snapshot.page + 1 }}</span>
            <button class="button" :disabled="!snapshot.has_next || dashboard.busy || !!problem" @click="selectPage(snapshot.page + 1)">Next</button>
          </nav>
        </section>
        <section v-else-if="!problem" class="empty">
          <h2>No projects yet</h2>
          <p>Register a Laravel project with <code>roost project add PATH</code>.</p>
        </section>
      </template>
    </main>
  </div>
</template>
