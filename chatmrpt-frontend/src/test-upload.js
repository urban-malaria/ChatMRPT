// Test file upload functionality
const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');

async function testFileUpload() {
  console.log('Testing file upload to Flask backend...');
  
  try {
    // First, get a session
    const sessionResponse = await axios.post('http://localhost:5000/api/initialize_session');
    console.log('Session initialized:', sessionResponse.data.session_id);
    
    // Set session cookie
    const sessionId = sessionResponse.data.session_id;
    const cookie = `session=${sessionId}`;
    
    // Create form data
    const formData = new FormData();
    
    // Create a simple test CSV
    const csvContent = 'Ward,Population,Malaria_Cases\nWard1,1000,50\nWard2,2000,100\n';
    const csvBuffer = Buffer.from(csvContent);
    formData.append('csv_file', csvBuffer, 'test_data.csv');
    
    // Upload the file
    const uploadResponse = await axios.post(
      'http://localhost:5000/upload_both_files',
      formData,
      {
        headers: {
          ...formData.getHeaders(),
          'Cookie': cookie
        }
      }
    );
    
    console.log('Upload response:', uploadResponse.data);
    
    // Test sample data endpoint
    console.log('\nTesting sample data load...');
    const sampleResponse = await axios.post(
      'http://localhost:5000/load_sample_data',
      {},
      {
        headers: {
          'Cookie': cookie
        }
      }
    );
    
    console.log('Sample data response:', sampleResponse.data);
    
  } catch (error) {
    console.error('Test failed:', error.response?.data || error.message);
  }
}

// Run the test
testFileUpload();