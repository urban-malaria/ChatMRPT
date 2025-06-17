/**
 * DOM Helper Utilities Module
 * Common DOM manipulation and utility functions
 */

export class DOMHelpers {
    /**
     * Safely get element by ID
     * @param {string} id - Element ID
     * @returns {HTMLElement|null} Element or null
     */
    static getElementById(id) {
        return document.getElementById(id);
    }

    /**
     * Safely query selector
     * @param {string} selector - CSS selector
     * @param {HTMLElement} context - Context element (default: document)
     * @returns {HTMLElement|null} Element or null
     */
    static querySelector(selector, context = document) {
        return context.querySelector(selector);
    }

    /**
     * Safely query all selectors
     * @param {string} selector - CSS selector
     * @param {HTMLElement} context - Context element (default: document)
     * @returns {NodeList} Elements list
     */
    static querySelectorAll(selector, context = document) {
        return context.querySelectorAll(selector);
    }

    /**
     * Create element with attributes and content
     * @param {string} tagName - HTML tag name
     * @param {Object} attributes - Element attributes
     * @param {string|HTMLElement|Array} content - Element content
     * @returns {HTMLElement} Created element
     */
    static createElement(tagName, attributes = {}, content = '') {
        const element = document.createElement(tagName);
        
        // Set attributes
        Object.entries(attributes).forEach(([key, value]) => {
            if (key === 'className') {
                element.className = value;
            } else if (key === 'dataset') {
                Object.entries(value).forEach(([dataKey, dataValue]) => {
                    element.dataset[dataKey] = dataValue;
                });
            } else {
                element.setAttribute(key, value);
            }
        });

        // Set content
        if (typeof content === 'string') {
            element.innerHTML = content;
        } else if (content instanceof HTMLElement) {
            element.appendChild(content);
        } else if (Array.isArray(content)) {
            content.forEach(child => {
                if (typeof child === 'string') {
                    element.appendChild(document.createTextNode(child));
                } else if (child instanceof HTMLElement) {
                    element.appendChild(child);
                }
            });
        }

        return element;
    }

    /**
     * Add CSS classes to element
     * @param {HTMLElement} element - Target element
     * @param {...string} classes - CSS classes to add
     */
    static addClass(element, ...classes) {
        if (element && element.classList) {
            element.classList.add(...classes);
        }
    }

    /**
     * Remove CSS classes from element
     * @param {HTMLElement} element - Target element
     * @param {...string} classes - CSS classes to remove
     */
    static removeClass(element, ...classes) {
        if (element && element.classList) {
            element.classList.remove(...classes);
        }
    }

    /**
     * Toggle CSS class on element
     * @param {HTMLElement} element - Target element
     * @param {string} className - CSS class to toggle
     * @returns {boolean} True if class was added, false if removed
     */
    static toggleClass(element, className) {
        if (element && element.classList) {
            return element.classList.toggle(className);
        }
        return false;
    }

    /**
     * Check if element has CSS class
     * @param {HTMLElement} element - Target element
     * @param {string} className - CSS class to check
     * @returns {boolean} True if element has class
     */
    static hasClass(element, className) {
        return element && element.classList && element.classList.contains(className);
    }

    /**
     * Set element visibility
     * @param {HTMLElement} element - Target element
     * @param {boolean} visible - True to show, false to hide
     */
    static setVisible(element, visible) {
        if (element) {
            element.style.display = visible ? '' : 'none';
        }
    }

    /**
     * Show element
     * @param {HTMLElement} element - Target element
     */
    static show(element) {
        this.setVisible(element, true);
    }

    /**
     * Hide element
     * @param {HTMLElement} element - Target element
     */
    static hide(element) {
        this.setVisible(element, false);
    }

    /**
     * Escape HTML content to prevent XSS
     * @param {string} str - String to escape
     * @returns {string} Escaped string
     */
    static escapeHTML(str) {
        if (typeof str !== 'string') return '';
        
        const div = document.createElement('div');
        div.textContent = str;
        return div.innerHTML;
    }

    /**
     * Scroll element to bottom smoothly
     * @param {HTMLElement} element - Target element
     */
    static scrollToBottom(element) {
        if (element) {
            element.scrollTo({
                top: element.scrollHeight,
                behavior: 'smooth'
            });
        }
    }

    /**
     * Scroll element into view
     * @param {HTMLElement} element - Target element
     * @param {boolean} smooth - Use smooth scrolling
     */
    static scrollIntoView(element, smooth = true) {
        if (element && element.scrollIntoView) {
            element.scrollIntoView({
                behavior: smooth ? 'smooth' : 'auto',
                block: 'nearest'
            });
        }
    }

    /**
     * Get element position relative to document
     * @param {HTMLElement} element - Target element
     * @returns {Object} Position object with top, left, width, height
     */
    static getElementPosition(element) {
        if (!element) return { top: 0, left: 0, width: 0, height: 0 };
        
        const rect = element.getBoundingClientRect();
        const scrollTop = window.pageYOffset || document.documentElement.scrollTop;
        const scrollLeft = window.pageXOffset || document.documentElement.scrollLeft;
        
        return {
            top: rect.top + scrollTop,
            left: rect.left + scrollLeft,
            width: rect.width,
            height: rect.height
        };
    }

    /**
     * Add event listener with optional delegation
     * @param {HTMLElement|string} target - Target element or selector for delegation
     * @param {string} event - Event type
     * @param {Function} handler - Event handler
     * @param {HTMLElement} context - Context for delegation (default: document)
     */
    static addEventListenerWithDelegation(target, event, handler, context = document) {
        if (typeof target === 'string') {
            // Event delegation
            context.addEventListener(event, function(e) {
                if (e.target.matches(target) || e.target.closest(target)) {
                    handler.call(e.target.closest(target) || e.target, e);
                }
            });
        } else if (target instanceof HTMLElement) {
            // Direct event listener
            target.addEventListener(event, handler);
        }
    }

    /**
     * Remove all child elements from parent
     * @param {HTMLElement} parent - Parent element
     */
    static clearChildren(parent) {
        if (parent) {
            while (parent.firstChild) {
                parent.removeChild(parent.firstChild);
            }
        }
    }

    /**
     * Animate element using CSS classes
     * @param {HTMLElement} element - Target element
     * @param {string} animationClass - CSS animation class
     * @param {Function} callback - Callback when animation ends
     */
    static animateElement(element, animationClass, callback) {
        if (!element) return;

        const onAnimationEnd = (e) => {
            element.classList.remove(animationClass);
            element.removeEventListener('animationend', onAnimationEnd);
            if (callback) callback();
        };

        element.addEventListener('animationend', onAnimationEnd);
        element.classList.add(animationClass);
    }

    /**
     * Debounce function execution
     * @param {Function} func - Function to debounce
     * @param {number} delay - Delay in milliseconds
     * @returns {Function} Debounced function
     */
    static debounce(func, delay) {
        let timeoutId;
        return function (...args) {
            clearTimeout(timeoutId);
            timeoutId = setTimeout(() => func.apply(this, args), delay);
        };
    }

    /**
     * Throttle function execution
     * @param {Function} func - Function to throttle
     * @param {number} delay - Delay in milliseconds
     * @returns {Function} Throttled function
     */
    static throttle(func, delay) {
        let timeoutId;
        let lastExecTime = 0;
        return function (...args) {
            const currentTime = Date.now();
            
            if (currentTime - lastExecTime > delay) {
                func.apply(this, args);
                lastExecTime = currentTime;
            } else {
                clearTimeout(timeoutId);
                timeoutId = setTimeout(() => {
                    func.apply(this, args);
                    lastExecTime = Date.now();
                }, delay - (currentTime - lastExecTime));
            }
        };
    }
}

export default DOMHelpers; 