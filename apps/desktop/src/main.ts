import { createCleanUI, restoreColorMode } from '@itguy614/clean-ui'
import '@itguy614/clean-ui/styles'
import { createPinia } from 'pinia'
import { createApp } from 'vue'
import App from './App.vue'
import './style.css'

restoreColorMode()

const showcase = import.meta.env.MODE === 'showcase' ||
  (import.meta.env.DEV && new URLSearchParams(location.search).has('showcase'))

async function mount() {
  const root = showcase ? (await import('./ThemeShowcase.vue')).default : App
  createApp(root).use(createPinia()).use(createCleanUI()).mount('#app')
}

void mount()
