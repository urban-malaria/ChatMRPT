import React, { useState } from 'react';
import LoginModal from './LoginModal';
import SignupModal from './SignupModal';
import PrivacyModal from '../Modal/PrivacyModal';

const LandingPage: React.FC = () => {
  const [showLoginModal, setShowLoginModal] = useState(false);
  const [showSignupModal, setShowSignupModal] = useState(false);
  const [showPrivacy, setShowPrivacy] = useState(false);

  const handleGoogleSignIn = () => {
    window.location.href = '/auth/google';
  };

  return (
    <>
      <PrivacyModal isOpen={showPrivacy} onAccept={() => setShowPrivacy(false)} />

      {/* Auth Modals */}
      <LoginModal
        isOpen={showLoginModal}
        onClose={() => setShowLoginModal(false)}
        onSwitchToSignup={() => {
          setShowLoginModal(false);
          setShowSignupModal(true);
        }}
      />
      <SignupModal
        isOpen={showSignupModal}
        onClose={() => setShowSignupModal(false)}
        onSwitchToLogin={() => {
          setShowSignupModal(false);
          setShowLoginModal(true);
        }}
      />

      {/* Landing Page */}
      <div className="min-h-screen bg-white flex flex-col items-center justify-center p-6">
        <div className="max-w-md w-full space-y-8 text-center">
          {/* Logo/Icon */}
          <div className="flex justify-center">
            <div className="w-16 h-16 bg-gradient-to-br from-blue-500 to-blue-700 rounded-2xl flex items-center justify-center shadow-lg">
              <svg className="w-10 h-10 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
            </div>
          </div>

          {/* Welcome Text */}
          <div className="space-y-3">
            <h1 className="text-4xl font-bold text-gray-900">
              Welcome to ChatMRPT
            </h1>
            <p className="text-lg text-gray-600">
              ChatMRPT (Malaria Risk Prioritization Tool) helps malaria programme staff in Nigeria
              find the wards at highest risk and plan bed net distribution, by chatting with their data
            </p>
          </div>

          {/* Features */}
          <div className="space-y-3 text-left bg-gray-50 rounded-xl p-6">
            <div className="flex items-start space-x-3">
              <svg className="w-5 h-5 text-blue-600 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
              </svg>
              <div>
                <p className="text-sm font-medium text-gray-900">Conversational Data Analysis</p>
                <p className="text-sm text-gray-600">Chat with your data using natural language</p>
              </div>
            </div>
            <div className="flex items-start space-x-3">
              <svg className="w-5 h-5 text-blue-600 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
              </svg>
              <div>
                <p className="text-sm font-medium text-gray-900">Malaria burden for every ward</p>
                <p className="text-sm text-gray-600">From health-facility testing data (DHIS2): test positivity rate (TPR) and cases per 1,000 people, with maps</p>
              </div>
            </div>
            <div className="flex items-start space-x-3">
              <svg className="w-5 h-5 text-blue-600 mt-0.5 flex-shrink-0" fill="currentColor" viewBox="0 0 20 20">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
              </svg>
              <div>
                <p className="text-sm font-medium text-gray-900">Ward risk ranking and bed net planning</p>
                <p className="text-sm text-gray-600">Rank wards by malaria risk and allocate insecticide-treated nets (ITNs) to the highest-risk wards first</p>
              </div>
            </div>
          </div>

          {/* CTA Buttons */}
          <div className="space-y-3 pt-4">
            <button
              onClick={handleGoogleSignIn}
              className="w-full px-6 py-3 bg-white text-gray-900 border-2 border-gray-200 rounded-lg hover:border-blue-500 hover:text-blue-600 transition-all font-medium text-base shadow-sm flex items-center justify-center gap-3"
            >
              <img src="https://www.gstatic.com/firebasejs/ui/2.0.0/images/auth/google.svg" alt="Google" className="w-5 h-5" />
              Continue with Google
            </button>
            <div className="flex items-center justify-center space-x-2 text-xs text-gray-400 uppercase tracking-wide">
              <span className="h-px w-10 bg-gray-200" />
              <span>or use email</span>
              <span className="h-px w-10 bg-gray-200" />
            </div>
            <button
              onClick={() => setShowLoginModal(true)}
              className="w-full px-6 py-3 bg-gray-900 text-white rounded-lg hover:bg-gray-800 transition-colors font-medium text-base shadow-sm"
            >
              Sign in with Email
            </button>
            <button
              onClick={() => setShowSignupModal(true)}
              className="w-full px-6 py-3 bg-white text-gray-900 border-2 border-gray-300 rounded-lg hover:bg-gray-50 hover:border-gray-400 transition-all font-medium text-base"
            >
              Create an Email Account
            </button>
          </div>

          {/* Footer */}
          <p className="text-xs text-gray-500 pt-6">
            How ChatMRPT handles your data is described in the{' '}
            <button type="button" onClick={() => setShowPrivacy(true)} className="underline hover:text-gray-700">privacy notice</button>.
          </p>
        </div>
      </div>
    </>
  );
};

export default LandingPage;
