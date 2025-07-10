// Debug script to check scrolling behavior
console.log('=== ChatMRPT Scroll Debug ===');

const chatContainer = document.querySelector('.chat-container');
const messagesContainer = document.getElementById('chat-messages');
const inputContainer = document.querySelector('.chat-input-container');

console.log('Chat Container:', {
    element: chatContainer,
    height: chatContainer?.offsetHeight,
    classes: chatContainer?.className,
    computedStyle: window.getComputedStyle(chatContainer)
});

console.log('Messages Container:', {
    element: messagesContainer,
    height: messagesContainer?.offsetHeight,
    scrollHeight: messagesContainer?.scrollHeight,
    clientHeight: messagesContainer?.clientHeight,
    scrollTop: messagesContainer?.scrollTop,
    overflow: window.getComputedStyle(messagesContainer)?.overflow,
    overflowY: window.getComputedStyle(messagesContainer)?.overflowY,
    flex: window.getComputedStyle(messagesContainer)?.flex
});

console.log('Input Container:', {
    element: inputContainer,
    height: inputContainer?.offsetHeight,
    flexShrink: window.getComputedStyle(inputContainer)?.flexShrink
});

// Check if scroll is possible
const canScroll = messagesContainer?.scrollHeight > messagesContainer?.clientHeight;
console.log('Can scroll:', canScroll);

// Try to force scroll to top
if (messagesContainer) {
    messagesContainer.scrollTop = 0;
    console.log('Forced scroll to top. New scrollTop:', messagesContainer.scrollTop);
}

// Add test scroll buttons
const testDiv = document.createElement('div');
testDiv.style.cssText = `
    position: fixed;
    top: 10px;
    right: 10px;
    z-index: 10000;
    background: white;
    padding: 10px;
    border: 2px solid blue;
    border-radius: 5px;
`;
testDiv.innerHTML = `
    <button onclick="document.getElementById('chat-messages').scrollTop = 0">Scroll Top</button>
    <button onclick="document.getElementById('chat-messages').scrollTop = document.getElementById('chat-messages').scrollHeight">Scroll Bottom</button>
    <button onclick="location.reload()">Reload</button>
`;
document.body.appendChild(testDiv);

console.log('Debug buttons added to top-right corner');