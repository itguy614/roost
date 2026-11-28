import { defineConfigWithVueTs, vueTsConfigs } from '@vue/eslint-config-typescript'
import vue from 'eslint-plugin-vue'

export default defineConfigWithVueTs(
  { ignores: ['dist/**', 'src-tauri/**'] },
  vue.configs['flat/essential'],
  vueTsConfigs.recommended,
)
