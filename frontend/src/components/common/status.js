import {STATUSES,PURGE_STATUSES} from '../../constants/enums.js';
const label=s=>STATUSES[s]||PURGE_STATUSES[s]||s;
export const status=s=>`<span class="status ${s}">${label(s)}</span>`;
