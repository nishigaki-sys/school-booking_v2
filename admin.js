import { initializeApp } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-app.js";
import { getAuth, signInAnonymously, signOut, onAuthStateChanged } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-auth.js";
import { getFirestore, doc, getDoc, setDoc, addDoc, updateDoc, deleteDoc, collection, onSnapshot, serverTimestamp, query, where, getDocs } from "https://www.gstatic.com/firebasejs/11.6.1/firebase-firestore.js";

// ==========================================
// 1. Config & State Management
// ==========================================
const GRADE_CONFIG = {
    preschool: { bg: 'bg-[#ff91aa]', text: 'text-white' },
    grade1_2: { bg: 'bg-[#ffac2d]', text: 'text-white' },
    grade3_plus: { bg: 'bg-[#00b4dc]', text: 'text-white' }
};

let auth, db, user, appId = 'robot-school-booking-v4';
let schools = [], currentSettings = {}, currentSchoolId = null;
let allBookings = [], allLogs = [], allInquiries = [];
let globalAllBookings = [];
let globalAllowedIps = [];
let commonContents = [];
let users = []; // 管理者ユーザーリスト

// Date Management
let adminDate = new Date();
let adminDashboardDate = new Date();
let selectedDateStr = "";
let scheduleToCopy = null;

// Listeners (for cleanup)
let unsubscribeSettings = null; 
let unsubscribeBookings = null;
let unsubscribeLogs = null;
let unsubscribeGlobal = null;
let unsubscribeInquiries = null;
let unsubscribeUsers = null;

// Charts
let bookingChart = null; 
let funnelChart = null; 
let schoolComparisonChart = null; 
let globalBookingChart = null; 
let contentPerformanceChart = null;
let trafficSourceChart = null; // NEW: 流入元チャート
let chartMode = 'created'; 
let globalChartMode = 'created'; 

// Current Login User Info
let currentUserData = null; // { email, role, assignedSchoolId, name }

// ==========================================
// 2. Initialization & Auth
// ==========================================
document.addEventListener('DOMContentLoaded', () => {
    initFirebase();
});

const fetchIpAddress = async () => {
    try {
        const response = await fetch('https://api.ipify.org?format=json');
        const data = await response.json();
        const ip = data.ip;
        const display = document.getElementById('currentIpDisplay');
        if (display) display.textContent = ip;
        return ip;
    } catch (e) {
        console.error("IP取得失敗", e);
        return null;
    }
};

const initFirebase = async () => {
    const config = { apiKey: "AIzaSyBYjDRWH0ldsP8WVHa9o8HcfGJ2XF9pdzU", authDomain: "robot-school-booking.firebaseapp.com", projectId: "robot-school-booking", appId: "1:443474003538:web:e3a17bd422bcee23f6418b" };
    const app = initializeApp(config);
    auth = getAuth(app);
    db = getFirestore(app);
    
    await signInAnonymously(auth);
    
    // Load Global Configs
    loadAllowedIps(db, appId);
    fetchIpAddress();
    loadCommonContents();

    // Event Bindings
    setupAuthEvents();
    setupNavigationEvents();
    setupGlobalAdminEvents();
    setupSchoolAdminEvents();
    setupUserManagementEvents(); // New
};

const loadAllowedIps = (db, appId) => {
    onSnapshot(doc(db, 'artifacts', appId, 'public', 'data', 'config', 'ip_allowlist'), (snap) => {
        if (snap.exists()) {
            globalAllowedIps = snap.data().list || [];
        } else {
            globalAllowedIps = [];
        }
    });
};

const setupAuthEvents = () => {
    // Login Form Submit
    document.getElementById('loginForm').onsubmit = async (e) => {
        e.preventDefault();
        
        // 1. IP Check
        if (globalAllowedIps.length > 0) {
            const currentIp = await fetchIpAddress();
            if (!currentIp || !globalAllowedIps.includes(currentIp)) {
                alert("許可されていないIPアドレスからのアクセスです。");
                return; 
            }
        }
        
        const email = document.getElementById('adminEmail').value.trim();
        const pass = document.getElementById('adminPassword').value.trim();

        if (!email || !pass) return alert("メールアドレスとパスワードを入力してください");

        try {
            // Firestoreからユーザーを検索
            const q = query(collection(db, 'artifacts', appId, 'public', 'data', 'users'), where("email", "==", email));
            const querySnapshot = await getDocs(q);
            
            let matchedUser = null;
            querySnapshot.forEach((doc) => {
                const u = doc.data();
                if (u.password === pass) { // 簡易的な平文パスワードチェック(Demo用)
                    matchedUser = { id: doc.id, ...u };
                }
            });

            if (matchedUser) {
                // Login Success
                currentUserData = matchedUser;
                document.getElementById('loginView').classList.add('hidden');
                document.getElementById('adminMain').classList.remove('hidden');
                
                // UI Update based on Role
                const infoEl = document.getElementById('loginUserInfo');
                infoEl.textContent = `${matchedUser.name} (${matchedUser.role === 'global' ? '本部' : '校舎'})`;
                infoEl.classList.remove('hidden');

                if (matchedUser.role === 'global') {
                    document.getElementById('globalAdminMenu').classList.remove('hidden');
                    loadUsers(); // ユーザー管理用データのロード
                } else {
                    document.getElementById('globalAdminMenu').classList.add('hidden');
                }

                loadSchools();
                initStatsDate();
                showToast(`ようこそ、${matchedUser.name}さん`);
            } else {
                alert("メールアドレスまたはパスワードが間違っています。");
            }
        } catch (err) {
            console.error(err);
            alert("ログイン処理中にエラーが発生しました: " + err.message);
        }
    };

    // Logout
    document.getElementById('logoutBtn').onclick = async () => {
        location.reload();
    };
};

// ==========================================
// 3. Global Admin Features
// ==========================================

// --- User Management (New Feature) ---
const setupUserManagementEvents = () => {
    document.getElementById('manageUsersBtn').onclick = () => {
        renderUserList();
        renderSchoolSelectOptions('userAssignedSchool'); // 担当校舎選択肢の更新
        resetUserForm();
        document.getElementById('userManagementModal').classList.remove('hidden');
    };

    document.getElementById('userManagementForm').onsubmit = async (e) => {
        e.preventDefault();
        const uid = document.getElementById('editUserId').value;
        const email = document.getElementById('userEmail').value.trim();
        const pass = document.getElementById('userPassword').value.trim();
        const name = document.getElementById('userName').value.trim();
        const role = document.getElementById('userRole').value;
        const schoolId = document.getElementById('userAssignedSchool').value;

        if (role === 'school' && !schoolId) return alert("校舎管理者の場合は、担当校舎を選択してください。");

        const userData = {
            email, password: pass, name, role, assignedSchoolId: role === 'school' ? schoolId : null,
            updatedAt: serverTimestamp()
        };

        try {
            if (uid) {
                // Update
                await updateDoc(doc(db, 'artifacts', appId, 'public', 'data', 'users', uid), userData);
                showToast("ユーザー情報を更新しました");
            } else {
                // Add
                // Check email duplicate (Client-side simple check)
                if (users.some(u => u.email === email)) return alert("このメールアドレスは既に登録されています");
                
                await addDoc(collection(db, 'artifacts', appId, 'public', 'data', 'users'), {
                    ...userData, createdAt: serverTimestamp()
                });
                showToast("新規ユーザーを追加しました");
            }
            resetUserForm();
        } catch(err) {
            alert("保存エラー: " + err.message);
        }
    };

    document.getElementById('cancelUserEditBtn').onclick = resetUserForm;

    // Role Select Change
    document.getElementById('userRole').onchange = (e) => {
        const schoolSelect = document.getElementById('schoolSelectContainer');
        if (e.target.value === 'school') schoolSelect.classList.remove('hidden');
        else schoolSelect.classList.add('hidden');
    };
};

const loadUsers = () => {
    if (unsubscribeUsers) unsubscribeUsers();
    unsubscribeUsers = onSnapshot(collection(db, 'artifacts', appId, 'public', 'data', 'users'), (snap) => {
        users = [];
        snap.forEach(d => users.push({ id: d.id, ...d.data() }));
        if (!document.getElementById('userManagementModal').classList.contains('hidden')) {
            renderUserList();
        }
    });
};

const renderUserList = () => {
    const con = document.getElementById('userListContainer');
    con.innerHTML = '';
    users.forEach(u => {
        const div = document.createElement('div');
        div.className = "p-3 border rounded bg-slate-50 text-sm flex justify-between items-center";
        
        let roleBadge = u.role === 'global' 
            ? '<span class="bg-indigo-100 text-indigo-700 px-2 py-0.5 rounded text-xs font-bold">本部</span>' 
            : '<span class="bg-teal-100 text-teal-700 px-2 py-0.5 rounded text-xs font-bold">校舎</span>';
        
        div.innerHTML = `
            <div>
                <div class="font-bold text-slate-700">${u.name} ${roleBadge}</div>
                <div class="text-xs text-slate-500">${u.email}</div>
            </div>
            <div class="flex gap-2">
                <button class="text-blue-500 text-xs font-bold edit-user-btn" data-id="${u.id}">編集</button>
                <button class="text-red-500 text-xs font-bold del-user-btn" data-id="${u.id}">削除</button>
            </div>
        `;
        con.appendChild(div);
    });

    con.querySelectorAll('.edit-user-btn').forEach(b => b.onclick = () => editUser(b.dataset.id));
    con.querySelectorAll('.del-user-btn').forEach(b => b.onclick = () => deleteUser(b.dataset.id));
};

const editUser = (id) => {
    const u = users.find(x => x.id === id);
    if (!u) return;
    document.getElementById('editUserId').value = u.id;
    document.getElementById('userEmail').value = u.email;
    document.getElementById('userPassword').value = u.password;
    document.getElementById('userName').value = u.name;
    document.getElementById('userRole').value = u.role;
    document.getElementById('userAssignedSchool').value = u.assignedSchoolId || "";
    
    // Trigger change to update UI
    document.getElementById('userRole').dispatchEvent(new Event('change'));
    
    document.getElementById('userFormTitle').textContent = "ユーザー編集";
    document.getElementById('cancelUserEditBtn').classList.remove('hidden');
};

const deleteUser = async (id) => {
    if (confirm("このユーザーを削除しますか？")) {
        await deleteDoc(doc(db, 'artifacts', appId, 'public', 'data', 'users', id));
        showToast("ユーザーを削除しました");
    }
};

const resetUserForm = () => {
    document.getElementById('editUserId').value = "";
    document.getElementById('userManagementForm').reset();
    document.getElementById('userFormTitle').textContent = "新規ユーザー追加";
    document.getElementById('cancelUserEditBtn').classList.add('hidden');
    document.getElementById('userRole').dispatchEvent(new Event('change'));
};

const renderSchoolSelectOptions = (selectId) => {
    const sel = document.getElementById(selectId);
    sel.innerHTML = '<option value="">選択してください</option>';
    schools.forEach(s => {
        const opt = document.createElement('option');
        opt.value = s.id;
        opt.textContent = s.name;
        sel.appendChild(opt);
    });
};

// --- IP Management ---
const setupGlobalAdminEvents = () => {
    // IP Management
    const renderIpList = () => {
        const list = document.getElementById('ipListContainer');
        list.innerHTML = '';
        globalAllowedIps.forEach((ip, index) => {
            const div = document.createElement('div');
            div.className = "flex justify-between items-center text-sm p-1 border-b last:border-b-0";
            div.innerHTML = `<span>${ip}</span><button class="text-red-500 hover:text-red-700 font-bold" onclick="window.removeIp(${index})">削除</button>`;
            list.appendChild(div);
        });
    };
    
    window.removeIp = (index) => {
        globalAllowedIps.splice(index, 1);
        renderIpList();
    };

    document.getElementById('manageIpBtn').onclick = () => {
        document.getElementById('ipSettingsModal').classList.remove('hidden');
        renderIpList();
    };

    document.getElementById('addIpBtn').onclick = () => {
        const ip = document.getElementById('newIpInput').value.trim();
        if (ip && !globalAllowedIps.includes(ip)) {
            globalAllowedIps.push(ip);
            document.getElementById('newIpInput').value = '';
            renderIpList();
        }
    };
    
    document.getElementById('addCurrentIpBtn').onclick = async () => {
         const ip = await fetchIpAddress();
         if(ip && !globalAllowedIps.includes(ip)) {
             globalAllowedIps.push(ip);
             renderIpList();
         }
    };

    document.getElementById('saveIpSettingsBtn').onclick = async () => {
         await setDoc(doc(db, 'artifacts', appId, 'public', 'data', 'config', 'ip_allowlist'), { list: globalAllowedIps });
         document.getElementById('ipSettingsModal').classList.add('hidden');
         showToast("IP制限設定を保存しました");
    };

    // Global Dashboard Events
    document.getElementById('openGlobalDashBtn').onclick = () => {
        document.getElementById('schoolSelectionView').classList.add('hidden');
        document.getElementById('globalDashboardView').classList.remove('hidden');
        document.getElementById('backToSchoolSelectBtn').classList.add('hidden');
        initGlobalDashboard();
    };

    document.getElementById('closeGlobalDashBtn').onclick = () => {
        if (unsubscribeGlobal) { unsubscribeGlobal(); unsubscribeGlobal = null; }
        document.getElementById('globalDashboardView').classList.add('hidden');
        document.getElementById('schoolSelectionView').classList.remove('hidden');
    };

    // Common Contents Events
    setupCommonContentsEvents();
};

const loadSchools = () => {
    onSnapshot(collection(db, 'artifacts', appId, 'public', 'data', 'schools'), (snap) => {
        schools = [];
        snap.forEach(d => schools.push({id:d.id, ...d.data()}));
        renderSchoolList();
    });
};

const renderSchoolList = () => {
    const con = document.getElementById('schoolListContainer');
    con.innerHTML = '';
    
    let displaySchools = schools;
    
    // Role based filtering
    if (currentUserData && currentUserData.role === 'school') {
        displaySchools = schools.filter(s => s.id === currentUserData.assignedSchoolId);
    }

    if (displaySchools.length === 0) {
        con.innerHTML = '<p class="text-slate-400 p-4">管理可能な校舎がありません。</p>';
        return;
    }

    displaySchools.forEach(s => {
        const div = document.createElement('div');
        div.className = "bg-white p-6 rounded-xl shadow-sm hover:shadow-md transition cursor-pointer border border-slate-200 relative group";
        
        let deleteBtn = '';
        if (currentUserData.role === 'global') {
            deleteBtn = `<button class="absolute top-2 right-2 text-slate-300 hover:text-red-500 delete-school" data-id="${s.id}"><svg xmlns="http://www.w3.org/2000/svg" class="h-5 w-5" viewBox="0 0 20 20" fill="currentColor"><path fill-rule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clip-rule="evenodd" /></svg></button>`;
        }

        div.innerHTML = `
            ${deleteBtn}
            <h3 class="text-xl font-bold text-slate-700 mb-1">${s.name}校</h3>
            <p class="text-xs text-slate-400">ID: ${s.id}</p>
            <div class="mt-4 text-blue-600 font-bold text-sm">管理画面へ &rarr;</div>
        `;
        div.onclick = (e) => {
            if(e.target.closest('.delete-school')) return;
            selectSchool(s.id);
        };
        
        if (currentUserData.role === 'global') {
            div.querySelector('.delete-school').onclick = async (e) => {
                e.stopPropagation();
                if(confirm('削除しますか？')) {
                    await deleteDoc(doc(db, 'artifacts', appId, 'public', 'data', 'schools', s.id));
                    await deleteDoc(doc(db, 'artifacts', appId, 'public', 'data', 'settings', s.id));
                }
            };
        }
        con.appendChild(div);
    });
};

const setupNavigationEvents = () => {
    document.getElementById('backToSchoolSelectBtn').onclick = () => {
        if (unsubscribeSettings) unsubscribeSettings();
        if (unsubscribeBookings) unsubscribeBookings();
        if (unsubscribeLogs) unsubscribeLogs();
        if (unsubscribeInquiries) unsubscribeInquiries();
        
        document.getElementById('schoolAdminView').classList.add('hidden');
        document.getElementById('schoolSelectionView').classList.remove('hidden');
        document.getElementById('backToSchoolSelectBtn').classList.add('hidden');
        document.getElementById('currentSchoolNameDisplay').classList.add('hidden');
        document.getElementById('previewPageBtn').classList.add('hidden');
        currentSchoolId = null;
    };
    
    // School Add Form (Global only)
    document.getElementById('addSchoolForm').onsubmit = async (e) => {
        e.preventDefault();
        const name = document.getElementById('newSchoolName').value;
        const id = document.getElementById('newSchoolId').value;
        if(schools.some(s=>s.id===id)) return alert("ID重複");
        await setDoc(doc(db, 'artifacts', appId, 'public', 'data', 'schools', id), { name, id });
        await setDoc(doc(db, 'artifacts', appId, 'public', 'data', 'settings', id), { 
            schoolName: name, schoolId: id, contents: [], schedule: {} 
        });
        document.getElementById('addSchoolModal').classList.add('hidden');
        e.target.reset();
    };
};

// ==========================================
// 4. Common Contents Management
// ==========================================
const loadCommonContents = () => {
    onSnapshot(doc(db, 'artifacts', appId, 'public', 'data', 'config', 'common_contents'), (snap) => {
        if (snap.exists()) {
            commonContents = snap.data().contents || [];
        } else {
            commonContents = [];
        }
        renderCommonContentsList();
    });
};

const renderCommonContentsList = () => {
    const con = document.getElementById('commonContentListContainer');
    con.innerHTML = '';
    if(commonContents.length === 0) {
        con.innerHTML = '<p class="text-xs text-slate-400 text-center py-4">登録された共通コンテンツはありません。</p>';
        return;
    }
    commonContents.forEach((c, idx) => {
        const div = document.createElement('div');
        div.className = "flex justify-between items-center p-3 bg-slate-50 border rounded text-sm";
        div.innerHTML = `
            <div>
                <div class="font-bold text-slate-700">${c.name}</div>
                <div class="text-xs text-slate-500">ID: ${c.id} | ¥${c.price}</div>
            </div>
            <div class="flex gap-2">
                <button class="text-blue-500 text-xs font-bold edit-common" data-idx="${idx}">編集</button>
                <button class="text-red-500 text-xs font-bold del-common" data-idx="${idx}">削除</button>
            </div>
        `;
        con.appendChild(div);
    });

    con.querySelectorAll('.edit-common').forEach(b => b.onclick = () => editCommonContent(b.dataset.idx));
    con.querySelectorAll('.del-common').forEach(b => b.onclick = async () => {
        if(confirm("削除しますか？")) {
            commonContents.splice(b.dataset.idx, 1);
            await setDoc(doc(db, 'artifacts', appId, 'public', 'data', 'config', 'common_contents'), { contents: commonContents });
            showToast("削除しました");
        }
    });
};

const editCommonContent = (idx) => {
    const c = commonContents[idx];
    document.getElementById('editCommonIdx').value = idx;
    document.getElementById('commonName').value = c.name;
    document.getElementById('commonId').value = c.id;
    document.getElementById('commonId').disabled = true;
    document.getElementById('commonType').value = c.type;
    document.getElementById('commonPrice').value = c.price;
    document.getElementById('commonImage').value = c.imageUrl || '';
    
    document.getElementById('commonDescription').value = c.description || '';
    document.getElementById('commonDuration').value = c.duration || '';
    document.getElementById('commonNotes').value = c.notes || '';
};

const setupCommonContentsEvents = () => {
    document.getElementById('manageCommonContentsBtn').onclick = () => {
        document.getElementById('commonContentsModal').classList.remove('hidden');
        // Reset form
        document.getElementById('editCommonIdx').value = -1;
        document.getElementById('commonName').value = '';
        document.getElementById('commonId').value = '';
        document.getElementById('commonId').disabled = false;
        document.getElementById('commonPrice').value = '';
        document.getElementById('commonImage').value = '';
        document.getElementById('commonDescription').value = '';
        document.getElementById('commonDuration').value = '';
        document.getElementById('commonNotes').value = '';
    };

    document.getElementById('saveCommonContentBtn').onclick = async () => {
        const idx = parseInt(document.getElementById('editCommonIdx').value);
        const name = document.getElementById('commonName').value;
        const idInput = document.getElementById('commonId').value.trim();
        const type = document.getElementById('commonType').value;
        const price = document.getElementById('commonPrice').value;
        const img = document.getElementById('commonImage').value;
        const description = document.getElementById('commonDescription').value;
        const duration = document.getElementById('commonDuration').value;
        const notes = document.getElementById('commonNotes').value;

        if(!name || !price) return alert("コンテンツ名と金額は必須です");

        let newId = idInput;
        if (!newId) newId = 'c' + Date.now();

        const newContent = { 
            id: newId, name, type, price: parseInt(price), imageUrl: img, 
            description, duration, notes
        };

        if (idx === -1) {
            if (commonContents.some(c => c.id === newId)) return alert("IDが重複しています");
            commonContents.push(newContent);
        } else {
            commonContents[idx] = newContent;
        }

        await setDoc(doc(db, 'artifacts', appId, 'public', 'data', 'config', 'common_contents'), { contents: commonContents });
        showToast("共通コンテンツを保存しました");
        
        // Reset
        document.getElementById('editCommonIdx').value = -1;
        document.getElementById('commonName').value = '';
        document.getElementById('commonId').value = '';
        document.getElementById('commonId').disabled = false;
        document.getElementById('commonPrice').value = '';
        document.getElementById('commonImage').value = '';
    };

    // Import Logic
    document.getElementById('openImportCommonModalBtn').onclick = () => {
        const list = document.getElementById('importListContainer');
        list.innerHTML = '';
        if(commonContents.length === 0) {
            list.innerHTML = '<p class="text-sm text-slate-400 p-2">共通コンテンツがありません。</p>';
        } else {
            commonContents.forEach(c => {
                const label = document.createElement('label');
                label.className = "flex items-center gap-3 p-2 hover:bg-slate-50 cursor-pointer border-b last:border-0";
                label.innerHTML = `
                    <input type="checkbox" class="import-check form-checkbox h-4 w-4 text-blue-600" value="${c.id}">
                    <div class="text-sm">
                        <div class="font-bold text-slate-700">${c.name}</div>
                        <div class="text-xs text-slate-500">ID: ${c.id} | ¥${c.price}</div>
                    </div>
                `;
                list.appendChild(label);
            });
        }
        document.getElementById('importCommonModal').classList.remove('hidden');
    };

    document.getElementById('execImportBtn').onclick = async () => {
        const checked = Array.from(document.querySelectorAll('.import-check:checked')).map(cb => cb.value);
        if(checked.length === 0) return alert("選択されていません");

        let count = 0;
        checked.forEach(id => {
            const target = commonContents.find(c => c.id === id);
            if(target) {
                if(!currentSettings.contents.some(lc => lc.id === target.id)) {
                    currentSettings.contents.push({...target});
                    count++;
                }
            }
        });

        if(count > 0) {
            await saveSettings();
            showToast(`${count}件のコンテンツを取り込みました`);
            renderLocalContents(); 
        } else {
            alert("選択されたコンテンツは既に取り込み済みか、存在しません。");
        }
        document.getElementById('importCommonModal').classList.add('hidden');
    };
};

// ==========================================
// 5. Global Dashboard
// ==========================================
const initGlobalStatsDate = () => {
    const today = new Date();
    const y = today.getFullYear();
    const m = today.getMonth();
    const firstDay = new Date(y, m, 1);
    const lastDay = new Date(y, m + 1, 0);
    const fmt = (d) => `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
    
    document.getElementById('gStatsStartDate').value = fmt(firstDay);
    document.getElementById('gStatsEndDate').value = fmt(lastDay);
};

const initGlobalDashboard = () => {
    initGlobalStatsDate();
    
    unsubscribeGlobal = onSnapshot(collection(db, 'artifacts', appId, 'public', 'data', 'bookings'), (snap) => {
        globalAllBookings = [];
        snap.forEach(doc => globalAllBookings.push({id: doc.id, ...doc.data()}));
        updateGlobalStats();
    });

    document.getElementById('calcGlobalStatsBtn').onclick = () => updateGlobalStats();

    document.querySelectorAll('.g-chart-tab-btn').forEach(btn => {
        btn.onclick = (e) => {
            document.querySelectorAll('.g-chart-tab-btn').forEach(b => {
                b.classList.remove('bg-white', 'text-indigo-600', 'shadow-sm');
                b.classList.add('text-slate-500');
            });
            e.target.classList.remove('text-slate-500');
            e.target.classList.add('bg-white', 'text-indigo-600', 'shadow-sm');
            globalChartMode = e.target.dataset.type;
            updateGlobalStats();
        };
    });
};

function updateGlobalStats() {
    const sDate = document.getElementById('gStatsStartDate').value;
    const eDate = document.getElementById('gStatsEndDate').value;
    
    document.getElementById('gTotalBookings').textContent = globalAllBookings.length;
    document.getElementById('gTotalSchools').textContent = schools.length;
    const yesterday = new Date(Date.now() - 86400000);
    const recentCount = globalAllBookings.filter(b => b.createdAt && b.createdAt.toDate() > yesterday).length;
    document.getElementById('gRecentActions').textContent = recentCount > 0 ? `+${recentCount}` : '0';

    let periodBookings = globalAllBookings;
    if(sDate && eDate) {
        periodBookings = globalAllBookings.filter(b => {
            let targetDate = '';
            if (globalChartMode === 'event') {
                targetDate = b.date;
            } else {
                if (b.createdAt && b.createdAt.toDate) {
                    const d = b.createdAt.toDate();
                    targetDate = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
                }
            }
            return targetDate >= sDate && targetDate <= eDate;
        });
    }

    document.getElementById('gPeriodBookingCount').textContent = periodBookings.length;
    const activeSchoolIds = new Set(periodBookings.map(b => b.schoolId));
    document.getElementById('gPeriodActiveSchools').textContent = activeSchoolIds.size;
    
    const dateCounts = {};
    periodBookings.forEach(b => {
        let targetDate = '';
        if (globalChartMode === 'event') {
            targetDate = b.date;
        } else {
            if (b.createdAt && b.createdAt.toDate) {
                const d = b.createdAt.toDate();
                targetDate = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
            }
        }
        if(targetDate) dateCounts[targetDate] = (dateCounts[targetDate] || 0) + 1;
    });
    const sortedDays = Object.entries(dateCounts).sort((a,b) => b[1] - a[1]);
    document.getElementById('gPeriodPeakDay').textContent = sortedDays.length > 0 ? `${sortedDays[0][0]} (${sortedDays[0][1]}件)` : '-';

    // Chart: School Comparison
    const schoolCounts = {};
    schools.forEach(s => schoolCounts[s.id] = 0);
    
    periodBookings.forEach(b => {
        const sId = b.schoolId || 'unknown';
        schoolCounts[sId] = (schoolCounts[sId] || 0) + 1;
    });
    
    const sortedSchools = Object.entries(schoolCounts).sort((a,b) => b[1] - a[1]);
    
    const chartLabels = sortedSchools.map(([id, count]) => {
        if (id === 'unknown') return '不明';
        const s = schools.find(school => school.id === id);
        return s ? s.name : `削除済・不明(${id})`;
    });

    if (schoolComparisonChart) schoolComparisonChart.destroy();
    const ctx = document.getElementById('schoolComparisonChart').getContext('2d');
    schoolComparisonChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: chartLabels,
            datasets: [{
                label: '予約数',
                data: sortedSchools.map(s => s[1]),
                backgroundColor: '#6366f1',
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { y: { beginAtZero: true, ticks: { stepSize: 1 } } }
        }
    });

    // Chart: Global Stacked
    if (sDate && eDate) {
        const schoolMap = {};
        schools.forEach(s => {
            schoolMap[s.id] = { label: s.name, data: {}, color: getColorForId(s.id) };
        });
        
        const labels = [];
        let d = new Date(sDate);
        const end = new Date(eDate);
        while(d <= end) {
            const dStr = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
            labels.push(dStr);
            for(let sid in schoolMap) schoolMap[sid].data[dStr] = 0;
            d.setDate(d.getDate() + 1);
        }

        globalAllBookings.forEach(b => {
            let targetDate = '';
            if(globalChartMode === 'event') {
                targetDate = b.date;
            } else {
                if(b.createdAt && b.createdAt.toDate) {
                    const cd = b.createdAt.toDate();
                    targetDate = `${cd.getFullYear()}-${String(cd.getMonth()+1).padStart(2,'0')}-${String(cd.getDate()).padStart(2,'0')}`;
                }
            }
            if(targetDate >= sDate && targetDate <= eDate) {
                if(schoolMap[b.schoolId] && schoolMap[b.schoolId].data[targetDate] !== undefined) {
                    schoolMap[b.schoolId].data[targetDate]++;
                }
            }
        });

        const datasets = Object.values(schoolMap).map(s => ({
            label: s.label,
            data: labels.map(l => s.data[l]),
            backgroundColor: s.color,
        })).filter(ds => ds.data.some(v => v > 0));

        if(globalBookingChart) globalBookingChart.destroy();
        const ctx2 = document.getElementById('globalBookingChart').getContext('2d');
        globalBookingChart = new Chart(ctx2, {
            type: 'bar',
            data: {
                labels: labels.map(l => l.substring(5)),
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: { stacked: true },
                    y: { stacked: true, beginAtZero: true, ticks: { stepSize: 1 } }
                },
                plugins: {
                    legend: { position: 'bottom', labels: { boxWidth: 12, font: { size: 10 } } }
                }
            }
        });
    }

    // Recent List
    const list = document.getElementById('globalRecentBookings');
    list.innerHTML = '';
    const sortedRecent = [...globalAllBookings].sort((a,b) => (b.createdAt?.seconds || 0) - (a.createdAt?.seconds || 0)).slice(0, 50);
    
    if (sortedRecent.length === 0) {
        list.innerHTML = '<div class="text-center text-slate-400 py-4">データがありません</div>';
    } else {
        sortedRecent.forEach(b => {
            const dateObj = b.createdAt ? b.createdAt.toDate() : new Date();
            const timeStr = `${dateObj.getMonth()+1}/${dateObj.getDate()} ${dateObj.getHours()}:${String(dateObj.getMinutes()).padStart(2,'0')}`;
            
            const div = document.createElement('div');
            div.className = "p-3 bg-slate-50 border border-slate-100 rounded hover:bg-slate-100 transition";
            div.innerHTML = `
                <div class="flex justify-between items-start">
                    <span class="text-xs font-bold text-slate-400">${timeStr}</span>
                    <span class="bg-indigo-100 text-indigo-700 text-[10px] px-2 py-0.5 rounded-full font-bold">${b.schoolName}</span>
                </div>
                <div class="font-bold text-slate-700 text-sm mt-1">${b.childName} 様</div>
                <div class="text-xs text-slate-500 mt-0.5">${b.courseName} (${b.date} ${b.startTime})</div>
            `;
            list.appendChild(div);
        });
    }
}

// ==========================================
// 6. School Admin Features
// ==========================================
function selectSchool(id) {
    if (unsubscribeSettings) unsubscribeSettings();
    if (unsubscribeBookings) unsubscribeBookings();
    if (unsubscribeLogs) unsubscribeLogs();
    if (unsubscribeInquiries) unsubscribeInquiries();

    currentSchoolId = id;
    document.getElementById('schoolSelectionView').classList.add('hidden');
    document.getElementById('schoolAdminView').classList.remove('hidden');
    document.getElementById('backToSchoolSelectBtn').classList.remove('hidden');
    document.getElementById('currentSchoolNameDisplay').classList.remove('hidden');
    document.getElementById('previewPageBtn').classList.remove('hidden');
    document.getElementById('previewPageBtn').href = `index.html?school=${id}`;
    
    // Load Settings
    unsubscribeSettings = onSnapshot(doc(db, 'artifacts', appId, 'public', 'data', 'settings', id), (snap) => {
        if(snap.exists()) currentSettings = snap.data();
        else currentSettings = { schoolName: schools.find(s=>s.id===id).name, contents:[], schedule:{} };
        
        if(!currentSettings.contents) currentSettings.contents = [];
        if(!currentSettings.schedule) currentSettings.schedule = {};

        document.getElementById('currentSchoolNameDisplay').textContent = currentSettings.schoolName;
        
        // Populate Form
        document.getElementById('settingSchoolName').value = currentSettings.schoolName || '';
        document.getElementById('settingAddress').value = currentSettings.address || '';
        document.getElementById('settingPhone').value = currentSettings.phoneNumber || '';
        document.getElementById('settingPageTitle').value = currentSettings.pageTitle || '';
        document.getElementById('settingPageDesc').value = currentSettings.pageDescription || '';
        document.getElementById('settingHeaderImage').value = currentSettings.headerImageUrl || '';
        
        renderLocalContents();
        renderSchedCalendar();
        updateStats(); 
    });
    
    // Load Bookings
    unsubscribeBookings = onSnapshot(collection(db, 'artifacts', appId, 'public', 'data', 'bookings'), (snap) => {
        allBookings = [];
        snap.forEach(d => { if(d.data().schoolId === id) allBookings.push({id:d.id, ...d.data()}); });
        renderDashboard();
        renderBookingTable();
        updateStats(); 
    });
    
    // Load Inquiries
    const inqQuery = query(collection(db, 'artifacts', appId, 'public', 'data', 'inquiries'), where("schoolId", "==", id));
    unsubscribeInquiries = onSnapshot(inqQuery, (snap) => {
        allInquiries = [];
        snap.forEach(d => allInquiries.push({id: d.id, ...d.data()}));
        renderInquiryTable();
    });

    // Load Logs
    const logsQuery = query(collection(db, 'artifacts', appId, 'public', 'data', 'access_logs'), where("schoolId", "==", id));
    unsubscribeLogs = onSnapshot(logsQuery, (snap) => {
        allLogs = [];
        snap.forEach(d => allLogs.push(d.data()));
        updateStats(); 
    });
}

const setupSchoolAdminEvents = () => {
    // Tabs
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.onclick = () => {
            document.querySelectorAll('.tab-btn').forEach(b => {
                b.classList.remove('border-blue-600', 'text-blue-600');
                b.classList.add('border-transparent', 'text-slate-500');
            });
            btn.classList.remove('border-transparent', 'text-slate-500');
            btn.classList.add('border-blue-600', 'text-blue-600');
            document.querySelectorAll('.tab-content').forEach(c => c.classList.add('hidden'));
            document.getElementById(`tab-${btn.dataset.tab}`).classList.remove('hidden');
            
            if(btn.dataset.tab === 'dashboard') renderDashboard(); 
            if(btn.dataset.tab === 'url-generator') updateGeneratedUrl();
        };
    });

    // Settings Save
    document.getElementById('schoolSettingsForm').onsubmit = async (e) => {
        e.preventDefault();
        const fd = new FormData(e.target);
        currentSettings.schoolName = fd.get('schoolName');
        currentSettings.address = fd.get('address');
        currentSettings.phoneNumber = fd.get('phoneNumber');
        currentSettings.pageTitle = fd.get('pageTitle');
        currentSettings.pageDescription = fd.get('pageDescription');
        currentSettings.headerImageUrl = fd.get('headerImageUrl');
        await saveSettings();
        
        await setDoc(doc(db, 'artifacts', appId, 'public', 'data', 'schools', currentSchoolId), { id: currentSchoolId, name: currentSettings.schoolName }, {merge: true});
        showToast("保存しました");
    };

    // Booking Add/Edit
    document.getElementById('openAddBookingModalBtn').onclick = () => {
        document.getElementById('addBookingModal').classList.remove('hidden');
        document.getElementById('addBookingForm').reset();
        document.getElementById('newBookingScheduleId').innerHTML = '<option value="">先に日付を選択してください</option>';
    };

    document.getElementById('newBookingDate').addEventListener('change', (e) => {
        updateScheduleSelect(e.target.value, 'newBookingScheduleId');
    });

    document.getElementById('addBookingForm').onsubmit = async (e) => {
        e.preventDefault();
        const date = document.getElementById('newBookingDate').value;
        const scheduleVal = document.getElementById('newBookingScheduleId').value;
        if (!date || !scheduleVal) return alert("日時とコースを選択してください");

        const scheduleData = JSON.parse(scheduleVal);

        try {
            await addDoc(collection(db, 'artifacts', appId, 'public', 'data', 'bookings'), {
                schoolId: currentSchoolId,
                schoolName: currentSettings.schoolName,
                date: date,
                startTime: scheduleData.startTime,
                contentId: scheduleData.contentId,
                courseName: scheduleData.courseName,
                childName: document.getElementById('newChildName').value,
                parentName: document.getElementById('newParentName').value,
                email: document.getElementById('newEmail').value,
                phone: document.getElementById('newPhone').value,
                grade: document.getElementById('newGrade').value,
                sourceType: 'admin',
                createdAt: serverTimestamp()
            });
            document.getElementById('addBookingModal').classList.add('hidden');
            showToast("予約を登録しました");
        } catch (err) {
            alert("登録失敗: " + err.message);
        }
    };
    
    // Booking Edit Form
    document.getElementById('editBookingDate').addEventListener('change', (e) => {
        updateScheduleSelect(e.target.value, 'editBookingScheduleId');
    });

    document.getElementById('bookingEditForm').onsubmit = async (e) => {
        e.preventDefault();
        const bookingId = document.getElementById('editBookingId').value;
        if (!bookingId) return;
        
        const updateData = {
            childName: document.getElementById('editChildName').value,
            parentName: document.getElementById('editParentName').value,
            email: document.getElementById('editEmail').value,
            phone: document.getElementById('editPhone').value
        };

        const newDate = document.getElementById('editBookingDate').value;
        const newScheduleVal = document.getElementById('editBookingScheduleId').value;
        
        if (newScheduleVal) {
              const schedData = JSON.parse(newScheduleVal);
              const origDate = document.getElementById('originalDate').value;
              const origStart = document.getElementById('originalStartTime').value;
              const origContent = document.getElementById('originalContentId').value;

              if (newDate !== origDate || schedData.startTime !== origStart || schedData.contentId !== origContent) {
                  updateData.date = newDate;
                  updateData.startTime = schedData.startTime;
                  updateData.contentId = schedData.contentId;
                  updateData.courseName = schedData.courseName;
              }
        }

        try {
            await updateDoc(doc(db, 'artifacts', appId, 'public', 'data', 'bookings', bookingId), updateData);
            document.getElementById('bookingEditModal').classList.add('hidden');
            showToast("予約情報を更新しました");
        } catch (err) {
            alert("更新に失敗しました: " + err.message);
        }
    };
    
    // URL Generator
    ['utmSource', 'utmMedium', 'utmCampaign'].forEach(id => {
        document.getElementById(id).addEventListener('input', updateGeneratedUrl);
    });
    
    document.getElementById('copyUrlBtn').onclick = () => {
        const urlInput = document.getElementById('generatedUrl');
        urlInput.select();
        urlInput.setSelectionRange(0, 99999); 
        document.execCommand('copy'); 
        showToast("URLをコピーしました");
    };

    // Schedule Events
    document.getElementById('schedPrev').onclick = () => { adminDate.setMonth(adminDate.getMonth()-1); renderSchedCalendar(); };
    document.getElementById('schedNext').onclick = () => { adminDate.setMonth(adminDate.getMonth()+1); renderSchedCalendar(); };
    document.getElementById('closeScheduleModal').onclick = () => document.getElementById('scheduleModal').classList.add('hidden');
    
    document.getElementById('schedCancelBtn').onclick = resetSchedForm;
    document.getElementById('schedSaveBtn').onclick = async () => {
        const idx = parseInt(document.getElementById('schedEditIndex').value);
        const cid = document.getElementById('schedContentSelect').value;
        const start = document.getElementById('schedStart').value;
        const end = document.getElementById('schedEnd').value;
        const cap = document.getElementById('schedCap').value;
        const gr = Array.from(document.querySelectorAll('.sched-grade:checked')).map(c=>c.value);
        
        if(!cid || !start || !end || gr.length===0) return alert("必須項目を入力してください");

        const [nh, nm] = start.split(':').map(Number);
        const [neh, nem] = end.split(':').map(Number);
        const newStartMins = nh * 60 + nm;
        const newEndMins = neh * 60 + nem;

        const existingEvents = currentSettings.schedule[selectedDateStr] || [];
        for (let i = 0; i < existingEvents.length; i++) {
            if (idx !== -1 && i === idx) continue; 
            const ex = existingEvents[i];
            const [exSh, exSm] = ex.startTime.split(':').map(Number);
            const [exEh, exEm] = ex.endTime.split(':').map(Number);
            const exStartMins = exSh * 60 + exSm;
            const exEndMins = exEh * 60 + exEm;

            if (newStartMins < exEndMins && newEndMins > exStartMins) {
                return alert("時間が重複しているため登録できません。");
            }
        }
        
        const newEvt = {
            id: idx !== -1 ? currentSettings.schedule[selectedDateStr][idx].id : 's'+Date.now(),
            contentId: cid, startTime: start, endTime: end, capacity: parseInt(cap), grades: gr
        };
        
        if(!currentSettings.schedule[selectedDateStr]) currentSettings.schedule[selectedDateStr] = [];
        
        if(idx !== -1) currentSettings.schedule[selectedDateStr][idx] = newEvt;
        else currentSettings.schedule[selectedDateStr].push(newEvt);
        
        currentSettings.schedule[selectedDateStr].sort((a,b)=>a.startTime.localeCompare(b.startTime));
        
        await saveSettings();
        renderTimeline(); renderDayList(); renderSchedCalendar();
        resetSchedForm();
    };

    document.getElementById('cancelCopyBtn').onclick = () => document.getElementById('copyDialog').classList.add('hidden');
    document.getElementById('execCopyBtn').onclick = async () => {
        const d = document.getElementById('copyDate').value;
        if(!d) return;
        if(!currentSettings.schedule[d]) currentSettings.schedule[d] = [];
        currentSettings.schedule[d].push({...scheduleToCopy, id: 's'+Date.now()});
        await saveSettings();
        document.getElementById('copyDialog').classList.add('hidden');
        showToast("複製しました");
        renderSchedCalendar();
    };

    // Content Events
    document.getElementById('closeContentModal').onclick = () => document.getElementById('contentModal').classList.add('hidden');
    document.getElementById('openContentModalBtn').onclick = () => openContentModal();

    document.getElementById('saveContentBtn').onclick = async () => {
        const idxStr = document.getElementById('editContentId').value;
        const name = document.getElementById('editContentName').value;
        const type = document.getElementById('editContentType').value;
        const price = document.getElementById('editContentPrice').value;
        const img = document.getElementById('editContentImage').value;
        const customId = document.getElementById('editContentCustomId').value.trim();
        const description = document.getElementById('editContentDescription').value;
        const duration = document.getElementById('editContentDuration').value;
        const notes = document.getElementById('editContentNotes').value;

        if(!name || !price) return alert("コンテンツ名と金額は必須です");
        
        let newId;
        if (idxStr === 'new') {
             if (customId) {
                 if (currentSettings.contents.some(c => c.id === customId)) {
                     return alert("指定されたIDは既に使用されています。");
                 }
                 newId = customId;
             } else {
                 newId = 'c' + Date.now();
             }
        } else {
            newId = currentSettings.contents[idxStr].id;
        }

        const newData = {
            id: newId, name, type, price: parseInt(price), imageUrl: img,
            description, duration, notes
        };
        
        if(idxStr === 'new') currentSettings.contents.push(newData);
        else currentSettings.contents[idxStr] = newData;
        
        await saveSettings();
        document.getElementById('contentModal').classList.add('hidden');
    };
};

// ==========================================
// 7. Render & Logic Functions
// ==========================================
// Dashboard
const initStatsDate = () => {
    const today = new Date();
    const y = today.getFullYear();
    const m = today.getMonth();
    const firstDay = new Date(y, m, 1);
    const lastDay = new Date(y, m + 1, 0);
    const fmt = (d) => `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
    document.getElementById('statsStartDate').value = fmt(firstDay);
    document.getElementById('statsEndDate').value = fmt(lastDay);
};

document.getElementById('calcStatsBtn').onclick = () => updateStats();
document.querySelectorAll('.chart-tab-btn').forEach(btn => {
    btn.onclick = (e) => {
        document.querySelectorAll('.chart-tab-btn').forEach(b => {
            b.classList.remove('bg-white', 'text-blue-600', 'shadow-sm');
            b.classList.add('text-slate-500');
        });
        e.target.classList.remove('text-slate-500');
        e.target.classList.add('bg-white', 'text-blue-600', 'shadow-sm');
        chartMode = e.target.dataset.type;
        updateChart();
    };
});

function updateStats() {
    const sDate = document.getElementById('statsStartDate').value;
    const eDate = document.getElementById('statsEndDate').value;
    if(!sDate || !eDate) return;
    
    const rangeBookings = allBookings.filter(b => b.date >= sDate && b.date <= eDate);
    const bookingCount = rangeBookings.length;
    
    let capacityCount = 0;
    let d = new Date(sDate);
    const end = new Date(eDate);
    while(d <= end) {
        const dStr = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
        if(currentSettings.schedule && currentSettings.schedule[dStr]) {
            currentSettings.schedule[dStr].forEach(s => capacityCount += parseInt(s.capacity || 0));
        }
        d.setDate(d.getDate() + 1);
    }
    
    const rate = capacityCount > 0 ? Math.round((bookingCount / capacityCount) * 100) : 0;
    
    document.getElementById('statsBookingCount').textContent = bookingCount;
    document.getElementById('statsCapacityCount').textContent = capacityCount;
    document.getElementById('statsRate').textContent = `${rate}%`;
    
    updateChart();
    updateFunnelChart(sDate, eDate);
    updateContentPerformanceChart(sDate, eDate);
    updateTrafficSourceChart(sDate, eDate); // NEW
}

function updateContentPerformanceChart(sDate, eDate) {
    const statsMap = {};
    (currentSettings.contents || []).forEach(c => {
        statsMap[c.id] = { name: c.name, capacity: 0, booking: 0 };
    });
    statsMap['unknown'] = { name: 'その他・削除済', capacity: 0, booking: 0 };

    let d = new Date(sDate);
    const end = new Date(eDate);
    while(d <= end) {
        const dStr = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
        if(currentSettings.schedule && currentSettings.schedule[dStr]) {
            currentSettings.schedule[dStr].forEach(evt => {
                const cid = statsMap[evt.contentId] ? evt.contentId : 'unknown';
                statsMap[cid].capacity += parseInt(evt.capacity || 0);
            });
        }
        d.setDate(d.getDate() + 1);
    }

    allBookings.forEach(b => {
        if(b.date >= sDate && b.date <= eDate) {
            const cid = statsMap[b.contentId] ? b.contentId : 'unknown';
            statsMap[cid].booking += 1;
        }
    });

    const labels = [];
    const dataCapacity = [];
    const dataBooking = [];

    Object.values(statsMap).forEach(item => {
        if (item.capacity > 0 || item.booking > 0) {
            labels.push(item.name);
            dataCapacity.push(item.capacity);
            dataBooking.push(item.booking);
        }
    });

    if (contentPerformanceChart) contentPerformanceChart.destroy();
    const ctx = document.getElementById('contentPerformanceChart').getContext('2d');

    contentPerformanceChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: '予約数',
                    data: dataBooking,
                    backgroundColor: '#f97316', 
                    borderRadius: 4,
                    order: 1, // 手前に表示
                    barPercentage: 0.5, // 少し細くする
                    categoryPercentage: 0.8
                },
                {
                    label: '総枠数(定員)',
                    data: dataCapacity,
                    backgroundColor: '#cbd5e1',
                    borderRadius: 4,
                    order: 2, // 奥に表示
                    barPercentage: 0.8, // 太くする
                    categoryPercentage: 0.8
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { position: 'top' },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            let label = context.dataset.label || '';
                            if (label) label += ': ';
                            if (context.parsed.y !== null) label += context.parsed.y;
                            if(context.dataset.label === '予約数') {
                                const index = context.dataIndex;
                                const cap = dataCapacity[index];
                                const val = context.parsed.y;
                                if(cap > 0) {
                                    const rate = Math.round((val / cap) * 100);
                                    label += ` (${rate}%)`;
                                }
                            }
                            return label;
                        }
                    }
                }
            },
            scales: { 
                x: { stacked: false }, // 積み上げ無効化
                y: { beginAtZero: true } 
            }
        }
    });
}

function updateFunnelChart(sDate, eDate) {
    const rangeLogs = allLogs.filter(log => {
        if (log.timestamp && log.timestamp.toDate) {
            const d = log.timestamp.toDate();
            const dStr = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
            return dStr >= sDate && dStr <= eDate;
        }
        return false;
    });

    const counts = { page_view: 0, grade_selection: 0, date_click: 0, content_selection: 0, form_input: 0, conversion: 0 };
    rangeLogs.forEach(log => { if (counts[log.event] !== undefined) counts[log.event]++; });

    const data = [counts.page_view, counts.grade_selection, counts.date_click, counts.content_selection, counts.form_input, counts.conversion];
    const labels = ['ページPV', '学年選択', '日程クリック', 'コンテンツ選択', 'フォーム入力', '申込完了'];
    const cvr = counts.page_view > 0 ? ((counts.conversion / counts.page_view) * 100).toFixed(2) : 0;
    document.getElementById('analyticsCvr').textContent = `${cvr}%`;

    const listContainer = document.getElementById('analyticsList');
    listContainer.innerHTML = '';
    
    data.forEach((val, i) => {
        if (i === 0) return; 
        const prev = data[i-1];
        const row = document.createElement('div');
        row.className = "flex justify-between items-center bg-slate-50 px-2 py-1 rounded";
        row.innerHTML = `
            <span class="text-xs font-bold">${labels[i]}</span>
            <div class="flex items-center gap-2">
                <span class="text-xs text-slate-500">${val}</span>
                <span class="text-[10px] px-1 bg-white border rounded text-blue-600 font-bold">TOP比 ${Math.round(counts.page_view > 0 ? val/counts.page_view*100 : 0)}%</span>
            </div>
        `;
        listContainer.appendChild(row);
    });
    
    const pvRow = document.createElement('div');
    pvRow.className = "flex justify-between items-center px-2 py-1 border-b border-slate-100 mb-1";
    pvRow.innerHTML = `<span class="text-xs font-bold">ページPV</span><span class="text-xs font-bold text-slate-700">${counts.page_view}</span>`;
    listContainer.prepend(pvRow);

    if (funnelChart) funnelChart.destroy();
    const ctx = document.getElementById('funnelChart').getContext('2d');
    funnelChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: 'アクセス数',
                data: data,
                backgroundColor: ['#e2e8f0', '#cbd5e1', '#94a3b8', '#64748b', '#475569', '#3b82f6'],
                borderRadius: 4, barPercentage: 0.6
            }]
        },
        options: {
            indexAxis: 'y', responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: { x: { display: false }, y: { grid: { display: false } } }
        }
    });
}

function updateTrafficSourceChart(sDate, eDate) {
    const rangeBookings = allBookings.filter(b => b.date >= sDate && b.date <= eDate);
    
    const sourceCounts = {};
    rangeBookings.forEach(b => {
        let label = 'Direct / None';
        if (b.sourceType === 'admin') {
            label = '管理画面登録';
        } else if (b.utmSource) {
            label = b.utmSource;
            if (b.utmMedium) label += ` (${b.utmMedium})`;
        } else {
            label = 'Web予約 (Direct)';
        }
        sourceCounts[label] = (sourceCounts[label] || 0) + 1;
    });

    const labels = Object.keys(sourceCounts);
    const data = Object.values(sourceCounts);
    
    if (trafficSourceChart) trafficSourceChart.destroy();
    const ctx = document.getElementById('trafficSourceChart').getContext('2d');
    
    const colors = [
        '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6', '#ec4899', '#6366f1', '#14b8a6'
    ];

    trafficSourceChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: labels,
            datasets: [{
                data: data,
                backgroundColor: colors.slice(0, labels.length),
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { position: 'right', labels: { boxWidth: 10, font: { size: 10 } } }
            },
            cutout: '60%'
        }
    });
}

function updateChart() {
    const sDate = document.getElementById('statsStartDate').value;
    const eDate = document.getElementById('statsEndDate').value;
    if(!sDate || !eDate) return;

    const contentMap = {};
    (currentSettings.contents || []).forEach(c => {
        contentMap[c.id] = { label: c.name, data: {}, color: getColorForId(c.id) };
    });
    contentMap['unknown'] = { label: 'その他', data: {}, color: '#94a3b8' };
    
    const labels = [];
    let d = new Date(sDate);
    const end = new Date(eDate);
    while(d <= end) {
        const dStr = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
        labels.push(dStr);
        for(let cid in contentMap) contentMap[cid].data[dStr] = 0;
        d.setDate(d.getDate() + 1);
    }

    allBookings.forEach(b => {
        let targetDate = '';
        if(chartMode === 'event') {
            targetDate = b.date;
        } else {
            if(b.createdAt && b.createdAt.toDate) {
                const cd = b.createdAt.toDate();
                targetDate = `${cd.getFullYear()}-${String(cd.getMonth()+1).padStart(2,'0')}-${String(cd.getDate()).padStart(2,'0')}`;
            }
        }
        
        if(targetDate >= sDate && targetDate <= eDate) {
            const cid = (currentSettings.contents || []).find(c => c.id === b.contentId) ? b.contentId : 'unknown';
            if(contentMap[cid] && contentMap[cid].data[targetDate] !== undefined) {
                contentMap[cid].data[targetDate]++;
            }
        }
    });

    const datasets = Object.values(contentMap).map(c => ({
        label: c.label,
        data: labels.map(l => c.data[l]),
        backgroundColor: c.color,
    })).filter(ds => ds.data.some(v => v > 0));

    if(bookingChart) bookingChart.destroy();
    const ctx = document.getElementById('bookingChart').getContext('2d');
    bookingChart = new Chart(ctx, {
        type: 'bar',
        data: { labels: labels.map(l => l.substring(5)), datasets: datasets },
        options: {
            responsive: true, maintainAspectRatio: false,
            scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true, ticks: { stepSize: 1 } } },
            plugins: { legend: { position: 'bottom', labels: { boxWidth: 12, font: { size: 10 } } } }
        }
    });
}

// Inquiries
function renderInquiryTable() {
    const tbody = document.getElementById('inquiryTableBody');
    tbody.innerHTML = '';
    
    allInquiries.sort((a,b) => (b.createdAt?.seconds || 0) - (a.createdAt?.seconds || 0));

    if (allInquiries.length === 0) {
        tbody.innerHTML = '<tr><td colspan="5" class="px-6 py-8 text-center text-slate-400 text-sm">お問い合わせはありません。</td></tr>';
        return;
    }

    allInquiries.forEach(inq => {
        const tr = document.createElement('tr');
        tr.className = "hover:bg-slate-50 border-b last:border-0";
        
        let dateStr = '-';
        if(inq.createdAt && inq.createdAt.toDate) {
            const d = inq.createdAt.toDate();
            dateStr = `${d.getFullYear()}/${d.getMonth()+1}/${d.getDate()} ${d.getHours()}:${String(d.getMinutes()).padStart(2,'0')}`;
        }

        const status = inq.status || 'pending';
        let selectBg = 'bg-white';
        if (status === 'pending') selectBg = 'bg-red-50 text-red-700 border-red-200';
        else if (status === 'in_progress') selectBg = 'bg-yellow-50 text-yellow-700 border-yellow-200';
        else if (status === 'completed') selectBg = 'bg-gray-50 text-gray-500 border-gray-200';

        tr.innerHTML = `
            <td class="px-6 py-4 text-xs font-mono text-slate-500 whitespace-nowrap">${dateStr}</td>
            <td class="px-6 py-4 font-bold text-slate-700">${inq.name} 様</td>
            <td class="px-6 py-4 text-xs text-slate-600">
                <div>${inq.email}</div>
                <div>${inq.phone}</div>
            </td>
            <td class="px-6 py-4 text-sm text-slate-600 whitespace-pre-wrap">${inq.preferredSchedule}</td>
            <td class="px-6 py-4 text-center">
                <select onchange="window.updateInquiryStatus('${inq.id}', this.value)" class="text-xs p-1 rounded border ${selectBg} font-bold focus:outline-none focus:ring-2 focus:ring-blue-300 transition-colors cursor-pointer">
                    <option value="pending" ${status === 'pending' ? 'selected' : ''}>未対応</option>
                    <option value="in_progress" ${status === 'in_progress' ? 'selected' : ''}>対応中</option>
                    <option value="completed" ${status === 'completed' ? 'selected' : ''}>完了</option>
                </select>
            </td>
        `;
        tbody.appendChild(tr);
    });
}

window.updateInquiryStatus = async (id, status) => {
    try {
        await updateDoc(doc(db, 'artifacts', appId, 'public', 'data', 'inquiries', id), { status: status });
        showToast("ステータスを更新しました");
    } catch (err) {
        alert("更新に失敗しました: " + err.message);
    }
};

// Render Functions (Dashboard & Booking List)
function renderDashboard() {
    const total = allBookings.length;
    document.getElementById('totalBookings').textContent = total;
    const m = new Date();
    const mPrefix = `${m.getFullYear()}-${String(m.getMonth()+1).padStart(2,'0')}`;
    document.getElementById('monthBookings').textContent = allBookings.filter(b=>b.date.startsWith(mPrefix)).length;
    
    let monthSales = 0;
    allBookings.forEach(b => {
        if(b.date.startsWith(mPrefix)) {
            const content = (currentSettings.contents || []).find(c => c.id === b.contentId);
            if(content && content.price) monthSales += parseInt(content.price);
        }
    });
    document.getElementById('monthSales').textContent = `¥${monthSales.toLocaleString()}`;
    
    document.getElementById('prevDashboardMonth').onclick = () => { adminDashboardDate.setMonth(adminDashboardDate.getMonth()-1); renderAdminDashboard(); };
    document.getElementById('nextDashboardMonth').onclick = () => { adminDashboardDate.setMonth(adminDashboardDate.getMonth()+1); renderAdminDashboard(); };
    renderAdminDashboard();
}

function renderAdminDashboard() {
     const y = adminDashboardDate.getFullYear(), m = adminDashboardDate.getMonth();
     document.getElementById('dashboardCalendarTitle').textContent = `${y}年 ${m+1}月`;
     const grid = document.getElementById('dashGrid');
     grid.innerHTML = '';
     
     const firstDay = new Date(y, m, 1).getDay();
     const days = new Date(y, m+1, 0).getDate();
     
     for(let i=0; i<firstDay; i++) grid.appendChild(document.createElement('div'));
     
     for(let d=1; d<=days; d++) {
         const dateStr = `${y}-${String(m+1).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
         const cell = document.createElement('div');
         cell.className = "bg-white dashboard-calendar-cell min-h-[50px] border-r border-b p-1";
         cell.innerHTML = `<div class="text-xs font-bold text-slate-500 mb-1">${d}</div>`;
         
         if(currentSettings.schedule[dateStr]) {
             currentSettings.schedule[dateStr].forEach(evt => {
                 const c = currentSettings.contents.find(x => x.id === evt.contentId) || {name:'?'};
                 const count = allBookings.filter(b => b.date === dateStr && b.startTime === evt.startTime && b.contentId === evt.contentId).length;
                 const remaining = Math.max(0, parseInt(evt.capacity) - count);
                 
                 const item = document.createElement('div');
                 let col = "bg-gray-400";
                 if(evt.grades && evt.grades.length > 0) {
                     if(evt.grades.length === 3) col = "bg-[#5abe50]";
                     else if(evt.grades.includes('preschool')) col = "bg-[#ff91aa]";
                     else if(evt.grades.includes('grade1_2')) col = "bg-[#ffac2d]";
                     else if(evt.grades.includes('grade3_plus')) col = "bg-[#00b4dc]";
                 }
                 item.className = `dashboard-event-item ${col}`;
                 item.innerHTML = `<div class="font-bold">${evt.startTime}</div><div class="truncate">${c.name}</div><div class="flex justify-between items-center mt-1"><span class="font-bold">予約:${count}/${evt.capacity}</span><span class="text-xs bg-white text-slate-600 px-1 rounded font-bold">残:${remaining}</span></div>`;
                 cell.appendChild(item);
              });
           }
           grid.appendChild(cell);
      }
}

function renderBookingTable() {
    const tbody = document.getElementById('bookingTableBody');
    tbody.innerHTML = '';
    allBookings.sort((a,b) => b.date.localeCompare(a.date)).forEach(b => {
        const tr = document.createElement('tr');
        tr.className = "hover:bg-slate-50";
        
        let sourceBadge = '';
        if (b.sourceType === 'admin') {
            sourceBadge = '<span class="bg-gray-100 text-gray-600 px-2 py-1 rounded text-xs border border-gray-200">管理画面</span>';
        } else {
            if (b.utmSource) {
                sourceBadge = `<span class="bg-indigo-50 text-indigo-600 px-2 py-1 rounded text-xs border border-indigo-100 block w-fit mb-1">${b.utmSource}</span>`;
                if(b.utmMedium) sourceBadge += `<span class="text-[10px] text-slate-400">${b.utmMedium}</span>`;
            } else {
                sourceBadge = '<span class="text-xs text-slate-400">Web予約</span>';
            }
        }

        tr.innerHTML = `
            <td class="px-6 py-4">${b.date} ${b.startTime}</td>
            <td class="px-6 py-4">${b.courseName}</td>
            <td class="px-6 py-4 font-bold">${b.childName} <span class="text-xs font-normal">(${b.parentName})</span></td>
            <td class="px-6 py-4 text-xs">${b.email}<br>${b.phone}</td>
            <td class="px-6 py-4 text-center"><span class="bg-green-100 text-green-700 px-2 py-1 rounded text-xs">確定</span></td>
            <td class="px-6 py-4 text-center">${sourceBadge}</td>
            <td class="px-6 py-4 text-center">
                <div class="flex flex-col gap-2">
                    <button onclick="window.openBookingEditModal('${b.id}')" class="text-xs bg-blue-50 text-blue-600 px-2 py-1 rounded border border-blue-200 hover:bg-blue-100 font-bold">編集</button>
                    <button onclick="window.cancelBooking('${b.id}')" class="text-xs bg-red-50 text-red-600 px-2 py-1 rounded border border-red-200 hover:bg-red-100 font-bold">キャンセル</button>
                </div>
            </td>
        `;
        tbody.appendChild(tr);
    });
}

// Booking Edit
window.openBookingEditModal = (bookingId) => {
    const booking = allBookings.find(b => b.id === bookingId);
    if (!booking) return;

    document.getElementById('editBookingId').value = booking.id;
    document.getElementById('editChildName').value = booking.childName || '';
    document.getElementById('editParentName').value = booking.parentName || '';
    document.getElementById('editEmail').value = booking.email || '';
    document.getElementById('editPhone').value = booking.phone || '';
    
    document.getElementById('originalDate').value = booking.date;
    document.getElementById('originalStartTime').value = booking.startTime;
    document.getElementById('originalContentId').value = booking.contentId;
    
    const dInput = document.getElementById('editBookingDate');
    dInput.value = booking.date; 
    
    const currentValObj = { contentId: booking.contentId, startTime: booking.startTime, courseName: booking.courseName };
    updateScheduleSelect(booking.date, 'editBookingScheduleId', JSON.stringify(currentValObj));
    
    document.getElementById('bookingEditModal').classList.remove('hidden');
};

window.cancelBooking = async (bookingId) => {
    if (!confirm("本当にこの予約をキャンセル（削除）しますか？\nこの操作は取り消せません。")) return;
    try {
        await deleteDoc(doc(db, 'artifacts', appId, 'public', 'data', 'bookings', bookingId));
        showToast("予約をキャンセルしました");
    } catch (err) {
        alert("削除に失敗しました: " + err.message);
    }
};

const updateScheduleSelect = (dateStr, selectElementId, currentVal = "") => {
    const sel = document.getElementById(selectElementId);
    sel.innerHTML = '<option value="">選択してください</option>';
    
    if (!dateStr || !currentSettings.schedule || !currentSettings.schedule[dateStr]) {
        if (dateStr) {
             const opt = document.createElement('option');
             opt.text = "開催予定がありません";
             sel.add(opt);
        }
        return;
    }

    currentSettings.schedule[dateStr].forEach(evt => {
        const c = currentSettings.contents.find(x => x.id === evt.contentId) || { name: 'Unknown' };
        const bookedCount = allBookings.filter(b => b.date === dateStr && b.startTime === evt.startTime && b.contentId === evt.contentId).length;
        const remaining = Math.max(0, parseInt(evt.capacity) - bookedCount);

        const opt = document.createElement('option');
        opt.value = JSON.stringify({ contentId: evt.contentId, startTime: evt.startTime, courseName: c.name });
        opt.textContent = `${evt.startTime} ${c.name} (残${remaining})`;
        
        if (currentVal) {
            try {
                const currentObj = JSON.parse(currentVal);
                 if (currentObj.contentId === evt.contentId && currentObj.startTime === evt.startTime) {
                    opt.selected = true;
                }
            } catch(e) {}
        }
        sel.appendChild(opt);
    });
};

const updateGeneratedUrl = () => {
    const source = document.getElementById('utmSource').value.trim();
    const medium = document.getElementById('utmMedium').value.trim();
    const campaign = document.getElementById('utmCampaign').value.trim();
    
    if (!currentSchoolId) return;

    let path = window.location.pathname;
    const lastSlash = path.lastIndexOf('/');
    if (lastSlash !== -1) path = path.substring(0, lastSlash + 1) + 'index.html';
    else path = '/index.html';

    const baseUrl = `${window.location.origin}${path}?school=${currentSchoolId}`;
    let finalUrl = baseUrl;
    const params = [];
    if (source) params.push(`utm_source=${encodeURIComponent(source)}`);
    if (medium) params.push(`utm_medium=${encodeURIComponent(medium)}`);
    if (campaign) params.push(`utm_campaign=${encodeURIComponent(campaign)}`);
    if (params.length > 0) finalUrl += '&' + params.join('&');
    document.getElementById('generatedUrl').value = finalUrl;
};

// Utils & Calendar Logic (Content Setting)
function renderLocalContents() {
    const con = document.getElementById('localContentList');
    con.innerHTML = '';
    (currentSettings.contents || []).forEach((c, idx) => {
        const div = document.createElement('div');
        div.className = "flex justify-between items-center p-3 bg-slate-50 border rounded text-sm";
        div.innerHTML = `<div><span class="font-bold text-slate-700">${c.name}</span> <span class="text-xs text-slate-500">${c.type === 'event' ? 'イベント' : '体験'} ¥${c.price}</span></div><div><button class="text-blue-500 text-xs mr-2 edit-cnt" data-idx="${idx}">編集</button><button class="text-red-500 text-xs del-cnt" data-idx="${idx}">削除</button></div>`;
        con.appendChild(div);
    });
    
    con.querySelectorAll('.edit-cnt').forEach(b => b.onclick = () => openContentModal(b.dataset.idx));
    con.querySelectorAll('.del-cnt').forEach(b => b.onclick = async () => {
        if(confirm("削除しますか？")) {
            const idx = parseInt(b.dataset.idx);
            currentSettings.contents.splice(idx, 1);
            await saveSettings();
        }
    });
}

function openContentModal(idxStr = null) {
    const isNew = idxStr === null;
    document.getElementById('contentFormTitle').textContent = isNew ? "新規作成" : "コンテンツ編集";
    document.getElementById('editContentId').value = isNew ? 'new' : idxStr;
    
    const idInput = document.getElementById('editContentCustomId');
    idInput.value = '';
    idInput.disabled = !isNew; 
    
    if (isNew) {
        document.getElementById('editContentName').value = '';
        document.getElementById('editContentPrice').value = '';
        document.getElementById('editContentImage').value = '';
        document.getElementById('editContentDescription').value = '';
        document.getElementById('editContentDuration').value = '';
        document.getElementById('editContentNotes').value = '';
    } else {
        const c = currentSettings.contents[idxStr];
        document.getElementById('editContentName').value = c.name;
        document.getElementById('editContentCustomId').value = c.id; 
        document.getElementById('editContentType').value = c.type;
        document.getElementById('editContentPrice').value = c.price;
        document.getElementById('editContentImage').value = c.imageUrl || '';
        document.getElementById('editContentDescription').value = c.description || '';
        document.getElementById('editContentDuration').value = c.duration || '';
        document.getElementById('editContentNotes').value = c.notes || '';
    }
    document.getElementById('contentModal').classList.remove('hidden');
}

function renderSchedCalendar() {
    const y = adminDate.getFullYear(), m = adminDate.getMonth();
    document.getElementById('schedTitle').textContent = `${y}年 ${m+1}月`;
    const grid = document.getElementById('schedGrid');
    grid.innerHTML = '';
    
    const firstDay = new Date(y, m, 1).getDay();
    const days = new Date(y, m+1, 0).getDate();
    
    for(let i=0; i<firstDay; i++) grid.appendChild(document.createElement('div'));
    
    for(let d=1; d<=days; d++) {
        const dateStr = `${y}-${String(m+1).padStart(2,'0')}-${String(d).padStart(2,'0')}`;
        const cell = document.createElement('div');
        cell.className = "bg-white min-h-[60px] p-1 border-r border-b hover:bg-yellow-50 cursor-pointer relative";
        cell.innerHTML = `<div class="font-bold text-slate-400 mb-1 text-xs">${d}</div>`;
        
        if(currentSettings.schedule[dateStr]) {
            currentSettings.schedule[dateStr].forEach(evt => {
                const c = currentSettings.contents.find(x => x.id === evt.contentId) || {name: '?'};
                let col = "bg-blue-500";
                if(evt.grades && evt.grades.length > 0) {
                    if(evt.grades.length === 3) col = "bg-[#5abe50] text-white";
                    else if(evt.grades.includes('preschool')) col = "bg-[#ff91aa] text-white";
                    else if(evt.grades.includes('grade1_2')) col = "bg-[#ffac2d] text-white";
                    else if(evt.grades.includes('grade3_plus')) col = "bg-[#00b4dc] text-white";
                }
                const div = document.createElement('div');
                div.className = `${col} rounded px-1 mb-1 text-[10px] truncate border border-white`;
                div.textContent = `${evt.startTime} ${c.name}`;
                cell.appendChild(div);
            });
        }
        cell.onclick = () => openScheduleModal(dateStr);
        grid.appendChild(cell);
    }
}

function openScheduleModal(dateStr) {
    selectedDateStr = dateStr; 
    document.getElementById('modalDateDisplay').textContent = dateStr;
    document.getElementById('scheduleModal').classList.remove('hidden');
    
    const sel = document.getElementById('schedContentSelect');
    sel.innerHTML = '';
    currentSettings.contents.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c.id; opt.textContent = `${c.name} (¥${c.price})`;
        sel.appendChild(opt);
    });
    
    resetSchedForm();
    renderTimeline();
    renderDayList();
}

function renderTimeline() {
    const tracks = document.getElementById('timelineTracks');
    const axis = document.getElementById('timelineAxis');
    tracks.innerHTML = ''; axis.innerHTML = '';
    
    const startHour = 9;
    const endHour = 20;
    const pxPerMin = 720 / ((endHour - startHour) * 60);

    for(let h=9; h<=20; h++) {
        const top = (h-9)*60 * pxPerMin;
        const l = document.createElement('div');
        l.className = "absolute w-full text-center transform -translate-y-1/2";
        l.style.top = top + 'px'; l.textContent = `${h}:00`;
        axis.appendChild(l);
        const line = document.createElement('div');
        line.className = "absolute w-full border-t border-slate-200 pointer-events-none";
        line.style.top = top + 'px';
        tracks.appendChild(line);
    }
    
    const evts = currentSettings.schedule[selectedDateStr] || [];
    evts.forEach((evt, idx) => {
        const c = currentSettings.contents.find(x => x.id === evt.contentId) || {name: 'Unknown'};
        const [sh, sm] = evt.startTime.split(':').map(Number);
        const [eh, em] = evt.endTime.split(':').map(Number);
        const sMin = (sh-9)*60 + sm;
        const eMin = (eh-9)*60 + em;
        const top = sMin * pxPerMin;
        const height = (eMin - sMin) * pxPerMin;
        
        const blk = document.createElement('div');
        let col = "bg-blue-500";
        if(evt.grades && evt.grades.length > 0) {
            if(evt.grades.length === 3) col = "bg-[#5abe50]";
            else if(evt.grades.includes('preschool')) col = "bg-[#ff91aa]";
            else if(evt.grades.includes('grade1_2')) col = "bg-[#ffac2d]";
            else if(evt.grades.includes('grade3_plus')) col = "bg-[#00b4dc]";
        }
        blk.className = `timeline-event-block ${col}`;
        blk.style.top = top+'px'; blk.style.height = `${Math.max(20, height)}px`;
        blk.style.left = '4px'; blk.style.right = '4px';
        blk.textContent = `${evt.startTime} ${c ? c.name : ''}`;
        blk.title = `${evt.startTime}-${evt.endTime} ${c ? c.name : ''}`;
        blk.onclick = (e) => { e.stopPropagation(); editSched(idx); };
        tracks.appendChild(blk);
    });

    const con = document.getElementById('timelineContainer');
    con.onclick = (e) => {
        if(e.target.closest('.timeline-event-block')) return;
        const rect = tracks.getBoundingClientRect();
        const y = e.clientY - rect.top; 
        const pxPerMin = 720 / 660;
        const mins = y / pxPerMin;
        const newTotal = 9*60 + mins;
        const rounded = Math.round(newTotal/10)*10;
        
        const h = Math.floor(rounded/60);
        const m = rounded%60;
        const eh = Math.floor((rounded+60)/60);
        const em = (rounded+60)%60;
        
        resetSchedForm();
        document.getElementById('schedStart').value = `${h.toString().padStart(2,'0')}:${m.toString().padStart(2,'0')}`;
        document.getElementById('schedEnd').value = `${eh.toString().padStart(2,'0')}:${em.toString().padStart(2,'0')}`;
    };
}

function renderDayList() {
    const list = document.getElementById('daySchedList');
    list.innerHTML = '';
    const evts = currentSettings.schedule[selectedDateStr] || [];
    evts.forEach((evt, idx) => {
        const c = currentSettings.contents.find(x => x.id === evt.contentId) || {name:'?'};
        const d = document.createElement('div');
        d.className = "flex justify-between items-center p-2 border rounded bg-white";
        d.innerHTML = `
            <div class="cursor-pointer" onclick="window.editSched(${idx})"><b>${evt.startTime}</b> ${c.name}</div>
            <div class="flex gap-1">
                <button class="text-blue-500 cp-btn" data-idx="${idx}">複製</button>
                <button class="text-red-500 del-s-btn" data-idx="${idx}">削除</button>
            </div>
        `;
        list.appendChild(d);
    });
    
    list.querySelectorAll('.cp-btn').forEach(b => b.onclick = (e) => {
        e.stopPropagation();
        scheduleToCopy = evts[b.dataset.idx];
        document.getElementById('copyDialog').classList.remove('hidden');
    });
    list.querySelectorAll('.del-s-btn').forEach(b => b.onclick = async (e) => {
        e.stopPropagation();
        const idx = parseInt(b.dataset.idx);
        const evt = evts[idx];
        const hasBooking = allBookings.some(booking => booking.date === selectedDateStr && booking.startTime === evt.startTime && booking.contentId === evt.contentId);
        if (hasBooking) return alert("この枠には既に予約が入っているため削除できません。\n予約管理タブから予約を確認・キャンセルしてください。");
        
        if(confirm("削除しますか？")) {
            currentSettings.schedule[selectedDateStr].splice(idx, 1);
            await saveSettings();
            renderTimeline(); renderDayList(); renderSchedCalendar();
        }
    });
}

window.editSched = (idx) => {
    const evt = currentSettings.schedule[selectedDateStr][idx];
    document.getElementById('schedEditIndex').value = idx;
    document.getElementById('schedContentSelect').value = evt.contentId;
    document.getElementById('schedStart').value = evt.startTime;
    document.getElementById('schedEnd').value = evt.endTime;
    document.getElementById('schedCap').value = evt.capacity;
    document.querySelectorAll('.sched-grade').forEach(c => c.checked = evt.grades && evt.grades.includes(c.value));
    document.getElementById('schedFormTitle').textContent = "枠を編集";
    document.getElementById('schedSaveBtn').textContent = "更新";
    document.getElementById('schedDeleteBtn').classList.remove('hidden');
    document.getElementById('schedCancelBtn').classList.remove('hidden');
};

const resetSchedForm = () => {
    document.getElementById('schedEditIndex').value = -1;
    document.getElementById('schedStart').value = '';
    document.getElementById('schedEnd').value = '';
    document.getElementById('schedCap').value = 5;
    document.querySelectorAll('.sched-grade').forEach(c => c.checked = false);
    document.getElementById('schedFormTitle').textContent = "新規枠を追加";
    document.getElementById('schedSaveBtn').textContent = "保存";
    document.getElementById('schedDeleteBtn').classList.add('hidden');
    document.getElementById('schedCancelBtn').classList.add('hidden');
};

// Common Helpers
async function saveSettings() {
    await setDoc(doc(db, 'artifacts', appId, 'public', 'data', 'settings', currentSchoolId), currentSettings);
}

function showToast(msg) {
    const t = document.getElementById('toast');
    t.textContent = msg;
    t.classList.remove('opacity-0');
    setTimeout(() => t.classList.add('opacity-0'), 3000);
}

function getColorForId(str) {
    let hash = 0;
    for (let i = 0; i < str.length; i++) hash = str.charCodeAt(i) + ((hash << 5) - hash);
    const c = (hash & 0x00FFFFFF).toString(16).toUpperCase();
    return '#' + "00000".substring(0, 6 - c.length) + c;
}
