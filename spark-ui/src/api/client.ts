import axios from "axios";


const defaultOptions = {
  baseURL: 'http://localhost:3001/api',
  headers: {
    'Content-Type': 'application/json',
    'Accept': 'application/json'
  },
};

export const useClient = () => {
 return axios.create(defaultOptions);

}
