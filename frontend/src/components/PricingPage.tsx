import React, { useState } from 'react';
import axios from 'axios';
import toast from 'react-hot-toast';

interface PricingPageProps {
  apiBase: string;
  authHeaders: Record<string, string>;
  onClose: () => void;
}

export const PricingPage: React.FC<PricingPageProps> = ({
  apiBase,
  authHeaders,
  onClose
}) => {
  const [email, setEmail] = useState('');
  const [selectedTier, setSelectedTier] = useState<string | null>(null);
  const [submitted, setSubmitted] = useState(false);

  const tiers = [
    {
      name: 'Free',
      price: '$0',
      period: 'forever',
      features: [
        '3 datasets max',
        '1M rows per dataset',
        '2GB storage',
        '10 AI queries/month',
        '7-day retention'
      ],
      cta: 'Current Plan',
      ctaAction: 'current',
      popular: false
    },
    {
      name: 'Pro',
      price: '$49',
      period: '/month',
      features: [
        'Unlimited datasets',
        '10M rows per dataset',
        '50GB storage',
        '100 AI queries/month',
        'CSV Merge ⭐',
        '30-day retention',
        'Priority support'
      ],
      cta: 'Join Waitlist',
      ctaAction: 'waitlist',
      popular: true
    },
    {
      name: 'Team',
      price: '$149',
      period: '/month',
      features: [
        'Everything in Pro',
        '5 team members',
        '100M rows per dataset',
        '200GB storage',
        'Unlimited AI queries',
        'Shared workspaces',
        'SSO (Google/Microsoft)'
      ],
      cta: 'Join Waitlist',
      ctaAction: 'waitlist',
      popular: false
    },
    {
      name: 'Enterprise',
      price: 'Custom',
      period: '',
      features: [
        'Everything in Team',
        'Unlimited seats',
        '1B rows per dataset',
        'Unlimited storage',
        'White-label option',
        '1-hour SLA support',
        'On-premise option'
      ],
      cta: 'Contact Sales',
      ctaAction: 'waitlist',
      popular: false
    }
  ];

  const joinWaitlist = async (tier: string) => {
    if (!email) {
      toast.error('Please enter your email');
      return;
    }

    try {
      await axios.post(
        `${apiBase}/waitlist`,
        {
          email,
          tier,
          timestamp: new Date().toISOString()
        },
        { headers: authHeaders }
      );

      setSubmitted(true);
      toast.success('✅ Added to waitlist!');
    } catch (error: any) {
      toast.error(error.response?.data?.detail || 'Failed to join waitlist');
    }
  };

  return (
    <div style={{
      position: 'fixed',
      inset: 0,
      background: 'rgba(0, 0, 0, 0.8)',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      zIndex: 1000,
      padding: '20px',
      overflowY: 'auto'
    }} onClick={onClose}>
      <div style={{
        background: '#1a1a24',
        borderRadius: '16px',
        width: '100%',
        maxWidth: '1200px',
        padding: '40px',
        maxHeight: '90vh',
        overflowY: 'auto'
      }} onClick={(e) => e.stopPropagation()}>
        
        <div style={{ marginBottom: '40px', textAlign: 'center' }}>
          <h1 style={{ margin: '0 0 12px 0', color: '#fff', fontSize: '36px' }}>
            Pricing
          </h1>
          <p style={{ margin: 0, color: '#a1a1aa', fontSize: '16px' }}>
            Currently in free beta • Join waitlist for paid tiers
          </p>
        </div>

        <div style={{
          display: 'grid',
          gridTemplateColumns: 'repeat(auto-fit, minmax(250px, 1fr))',
          gap: '24px',
          marginBottom: '40px'
        }}>
          {tiers.map(tier => (
            <div
              key={tier.name}
              style={{
                background: tier.popular ? '#2d2d44' : '#24243a',
                border: tier.popular ? '2px solid #6366f1' : '1px solid #2d2d44',
                borderRadius: '12px',
                padding: '32px 24px',
                position: 'relative'
              }}
            >
              {tier.popular && (
                <div style={{
                  position: 'absolute',
                  top: '-12px',
                  left: '50%',
                  transform: 'translateX(-50%)',
                  background: '#6366f1',
                  color: '#fff',
                  padding: '4px 16px',
                  borderRadius: '12px',
                  fontSize: '12px',
                  fontWeight: 600
                }}>
                  Most Popular
                </div>
              )}

              <h2 style={{ margin: '0 0 8px 0', color: '#fff', fontSize: '24px' }}>
                {tier.name}
              </h2>
              
              <div style={{ marginBottom: '24px' }}>
                <span style={{ fontSize: '36px', fontWeight: 700, color: '#fff' }}>
                  {tier.price}
                </span>
                <span style={{ fontSize: '14px', color: '#a1a1aa' }}>
                  {tier.period}
                </span>
              </div>

              <ul style={{
                listStyle: 'none',
                padding: 0,
                margin: '0 0 24px 0'
              }}>
                {tier.features.map(feature => (
                  <li key={feature} style={{
                    padding: '8px 0',
                    color: '#e4e4e7',
                    fontSize: '14px',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '8px'
                  }}>
                    <span style={{ color: '#10b981' }}>✓</span>
                    {feature}
                  </li>
                ))}
              </ul>

              {tier.ctaAction === 'current' ? (
                <button style={{
                  width: '100%',
                  padding: '12px',
                  background: '#3d3d54',
                  color: '#a1a1aa',
                  border: 'none',
                  borderRadius: '8px',
                  fontSize: '14px',
                  fontWeight: 600,
                  cursor: 'not-allowed'
                }}>
                  {tier.cta}
                </button>
              ) : (
                <button
                  onClick={() => setSelectedTier(tier.name)}
                  style={{
                    width: '100%',
                    padding: '12px',
                    background: tier.popular ? '#6366f1' : '#3d3d54',
                    color: '#fff',
                    border: 'none',
                    borderRadius: '8px',
                    fontSize: '14px',
                    fontWeight: 600,
                    cursor: 'pointer',
                    transition: 'all 0.2s'
                  }}
                  onMouseOver={(e) => {
                    e.currentTarget.style.opacity = '0.9';
                  }}
                  onMouseOut={(e) => {
                    e.currentTarget.style.opacity = '1';
                  }}
                >
                  {tier.cta}
                </button>
              )}
            </div>
          ))}
        </div>

        <div style={{
          background: '#24243a',
          borderRadius: '12px',
          padding: '24px',
          textAlign: 'center'
        }}>
          <p style={{ margin: '0 0 8px 0', color: '#e4e4e7', fontSize: '14px' }}>
            📅 Currently in free beta
          </p>
          <p style={{ margin: '0 0 8px 0', color: '#e4e4e7', fontSize: '14px' }}>
            🎯 Launching paid tiers when we hit 100 active users
          </p>
          <p style={{ margin: 0, color: '#10b981', fontSize: '14px', fontWeight: 600 }}>
            📊 23 people already on the waitlist
          </p>
        </div>

        <div style={{
          marginTop: '24px',
          textAlign: 'center'
        }}>
          <button
            onClick={onClose}
            style={{
              padding: '10px 24px',
              background: '#3d3d54',
              color: '#fff',
              border: 'none',
              borderRadius: '8px',
              fontSize: '14px',
              fontWeight: 600,
              cursor: 'pointer'
            }}
          >
            Close
          </button>
        </div>
      </div>

      {selectedTier && !submitted && (
        <div style={{
          position: 'fixed',
          inset: 0,
          background: 'rgba(0, 0, 0, 0.9)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          zIndex: 1001
        }} onClick={() => setSelectedTier(null)}>
          <div style={{
            background: '#1a1a24',
            borderRadius: '12px',
            padding: '32px',
            maxWidth: '400px',
            width: '90%'
          }} onClick={(e) => e.stopPropagation()}>
            <h3 style={{ margin: '0 0 16px 0', color: '#fff' }}>
              Join {selectedTier} Waitlist
            </h3>
            <p style={{ margin: '0 0 24px 0', color: '#a1a1aa', fontSize: '14px' }}>
              Get notified when {selectedTier} launches
            </p>

            <input
              type="email"
              placeholder="your@email.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              style={{
                width: '100%',
                padding: '12px',
                marginBottom: '16px',
                borderRadius: '8px',
                border: '1px solid #2d2d44',
                background: '#24243a',
                color: '#fff',
                fontSize: '14px'
              }}
            />

            <div style={{ display: 'flex', gap: '12px' }}>
              <button
                onClick={() => setSelectedTier(null)}
                style={{
                  flex: 1,
                  padding: '12px',
                  background: '#3d3d54',
                  color: '#fff',
                  border: 'none',
                  borderRadius: '8px',
                  fontSize: '14px',
                  fontWeight: 600,
                  cursor: 'pointer'
                }}
              >
                Cancel
              </button>
              <button
                onClick={() => joinWaitlist(selectedTier)}
                disabled={!email}
                style={{
                  flex: 1,
                  padding: '12px',
                  background: email ? '#6366f1' : '#3d3d54',
                  color: '#fff',
                  border: 'none',
                  borderRadius: '8px',
                  fontSize: '14px',
                  fontWeight: 600,
                  cursor: email ? 'pointer' : 'not-allowed'
                }}
              >
                Join Waitlist
              </button>
            </div>
          </div>
        </div>
      )}

      {submitted && (
        <div style={{
          position: 'fixed',
          top: '50%',
          left: '50%',
          transform: 'translate(-50%, -50%)',
          background: '#10b981',
          color: '#fff',
          padding: '20px 40px',
          borderRadius: '12px',
          fontSize: '16px',
          fontWeight: 600,
          zIndex: 1002
        }}>
          ✅ You're on the waitlist!
        </div>
      )}
    </div>
  );
};
