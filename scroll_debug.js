// Emergency scroll debug and fix
console.log('🚨 EMERGENCY SCROLL DEBUG');

const chatContainer = document.getElementById('chat-messages');
const messagesContainer = document.querySelector('.chat-messages-container');

if (chatContainer) {
    console.log('Chat container found:', {
        scrollHeight: chatContainer.scrollHeight,
        clientHeight: chatContainer.clientHeight,
        scrollTop: chatContainer.scrollTop,
        canScroll: chatContainer.scrollHeight > chatContainer.clientHeight
    });
    
    // Force scroll to top
    chatContainer.scrollTop = 0;
    console.log('✅ Forced scroll to top');
    
    // Add emergency scroll buttons
    const debugDiv = document.createElement('div');
    debugDiv.style.cssText = `
        position: fixed;
        top: 60px;
        right: 10px;
        z-index: 10000;
        background: red;
        color: white;
        padding: 10px;
        border-radius: 5px;
        font-size: 12px;
    `;
    debugDiv.innerHTML = `
        <div>Scroll Debug</div>
        <button onclick="document.getElementById('chat-messages').scrollTop = 0">📜 TOP</button>
        <button onclick="document.getElementById('chat-messages').scrollTop = document.getElementById('chat-messages').scrollHeight">📜 BOTTOM</button>
        <button onclick="console.log('Messages count:', document.querySelectorAll('.message').length)">📊 COUNT</button>
    `;
    document.body.appendChild(debugDiv);
    
    console.log('🔧 Debug buttons added');
} else {
    console.error('❌ Chat container not found!');
}

// Check for any CSS issues
const computedStyle = window.getComputedStyle(chatContainer);
console.log('📝 Container CSS:', {
    overflow: computedStyle.overflow,
    overflowY: computedStyle.overflowY,
    height: computedStyle.height,
    maxHeight: computedStyle.maxHeight,
    flex: computedStyle.flex
});