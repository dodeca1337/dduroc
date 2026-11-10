import { mount } from 'svelte';
import 'carbon-components-svelte/css/all.css';
import './fonts.css';
import './app.css';
import App from './App.svelte';
mount(App, { target: document.getElementById('app')! });
