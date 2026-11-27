<script setup lang="ts">
import {
  CuiAlert, CuiButton, CuiEmptyState, CuiList, CuiListItem, CuiPagination,
  CuiSidebar, CuiSidebarContent, CuiSidebarFooter, CuiSidebarHeader, CuiStatusDot,
  CuiTable, CuiTableBody, CuiTableCell, CuiTableHead, CuiTableRow, CuiTooltip,
} from '@itguy614/clean-ui'
import { computed, ref } from 'vue'
import icon from '../src-tauri/icons/icon.png'

const pages = [
  [{ name: 'Dispatch', observed: '2 / 2', status: 'At desired count', tone: 'success' },
    { name: 'Workshop', observed: '1 / 2', status: 'Below desired', tone: 'warning' }],
  [{ name: 'Support', observed: '—', status: 'Unavailable', tone: 'neutral' }],
] as const
const page = ref(1)
const rows = computed(() => pages[page.value - 1] ?? pages[0])
const showAlert = ref(true)
</script>

<template>
  <div class="shell antialiased">
    <CuiSidebar label="Theme examples" collapse-below="never">
      <CuiSidebarHeader title="Roost">
        <template #icon><img :src="icon" alt="" width="25" height="25"></template>
      </CuiSidebarHeader>
      <CuiSidebarContent>
        <CuiList>
          <CuiListItem title="Component showcase" href="#showcase" aria-current="page" />
          <CuiListItem title="Project table" href="#examples" />
        </CuiList>
      </CuiSidebarContent>
      <CuiSidebarFooter>
        <p class="sidebar-status"><CuiStatusDot tone="info" /> Development preview</p>
      </CuiSidebarFooter>
    </CuiSidebar>

    <main id="showcase">
      <header class="page-header">
        <div>
          <h1>Roost component showcase</h1>
          <p class="subtitle">Clean UI controls in the Roost theme</p>
        </div>
      </header>
      <section class="showcase-section" aria-labelledby="control-heading">
        <h2 id="control-heading">Buttons and feedback</h2>
        <div class="showcase-controls">
          <CuiTooltip text="Toggle the example alert">
            <CuiButton size="sm" variant="solid" :pressed="showAlert" @click="showAlert = !showAlert">Example alert</CuiButton>
          </CuiTooltip>
          <CuiButton size="sm" color="surface-dark" @click="page = 1">Reset page</CuiButton>
          <CuiButton size="sm" disabled>Unavailable action</CuiButton>
        </div>
      </section>
      <CuiAlert v-if="showAlert" title="Observed counts differ" color="warning" entrance="none">
        Workshop shows one running worker out of two desired.
      </CuiAlert>

      <section id="examples" class="showcase-section" aria-labelledby="table-heading">
        <h2 id="table-heading">Project table</h2>
        <CuiTable size="sm" hoverable>
          <caption class="sr-only">Example project observations</caption>
          <CuiTableHead>
            <CuiTableRow>
              <CuiTableCell scope="col">Project</CuiTableCell>
              <CuiTableCell scope="col">Running / desired</CuiTableCell>
              <CuiTableCell scope="col">Observation</CuiTableCell>
            </CuiTableRow>
          </CuiTableHead>
          <CuiTableBody>
            <CuiTableRow v-for="row in rows" :key="row.name">
              <CuiTableCell>{{ row.name }}</CuiTableCell>
              <CuiTableCell>{{ row.observed }}</CuiTableCell>
              <CuiTableCell>
                <span class="sidebar-status"><CuiStatusDot :tone="row.tone" /> {{ row.status }}</span>
              </CuiTableCell>
            </CuiTableRow>
          </CuiTableBody>
        </CuiTable>
        <CuiPagination v-model:current-page="page" class="pager" size="sm" :total-pages="pages.length" hide-per-page hide-info />
      </section>
      <CuiEmptyState size="sm" title-as="h2" title="No crash reports" description="New reports will appear here." />
    </main>
  </div>
</template>
