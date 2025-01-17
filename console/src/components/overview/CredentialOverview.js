import { useState, useEffect } from 'react';
import { Link as RouterLink } from 'react-router-dom';
import axios from 'axios';
import PerfectScrollbar from 'react-perfect-scrollbar';
import { x } from '@xstyled/emotion';
import { API_SERVER } from '../../config';
import {
  Box,
  Button,
  Card,
  CardHeader,
  Divider,
  Table,
  TableHead,
  TableBody,
  TableCell,
  TableRow,
  TableSortLabel
} from '@material-ui/core';
import ArrowRightIcon from '@material-ui/icons/ArrowRight';
import { useDispatch, useSelector } from 'react-redux';

import Refresh from '../common/Refresh';
import { getCredential } from '../store/actions/klevrActions';

const CredentialList = ({ sortedList }) => {
  const dispatch = useDispatch();
  const currentZone = useSelector((store) => store.zoneReducer);
  const credentialList = useSelector((store) => store.credentialReducer);

  const fetchCredential = () => {
    let completed = false;

    async function get() {
      const result = await axios.get(
        `${API_SERVER}/inner/groups/${currentZone}/credentials`
      );
      if (!completed) dispatch(getCredential(result.data));
    }
    get();
    return () => {
      completed = true;
    };
  };

  useEffect(() => {
    fetchCredential();
  }, []);

  useEffect(() => {
    fetchCredential();
  }, [currentZone]);

  if (!credentialList) {
    return null;
  }
  return (
    <TableBody>
      {sortedList.slice(0, 5).map((item) => (
        <TableRow hover key={item.id}>
          <TableCell>{`${item.key}`}</TableCell>
          <TableCell>{`${item.hash}`}</TableCell>
          <TableCell>{`${item.updatedAt}`}</TableCell>
          <TableCell>{`${item.createdAt}`}</TableCell>
        </TableRow>
      ))}
    </TableBody>
  );
};

const CredentialOverview = (props) => {
  const credentialList = useSelector((store) => store.credentialReducer);
  const [orderDirection, setOrderDirection] = useState('asc');
  const [valueToOrderBy, setValueToOrderBy] = useState('');

  const handleRequestSort = (e, property) => {
    const isAscending = valueToOrderBy === property && orderDirection === 'asc';
    setValueToOrderBy(property);
    setOrderDirection(isAscending ? 'desc' : 'asc');
  };

  const createSortHandler = (property) => (e) => {
    handleRequestSort(e, property);
  };

  function descendingComparator(a, b, orderBy) {
    if (b[orderBy] < a[orderBy]) {
      return -1;
    }
    if (b[orderBy] > a[orderBy]) {
      return 1;
    }
    return 0;
  }

  function getComparator(order, orderBy) {
    return order === 'desc'
      ? (a, b) => descendingComparator(a, b, orderBy)
      : (a, b) => -descendingComparator(a, b, orderBy);
  }

  function stableSort(array, comparator) {
    const stabilizedThis = array.map((el, index) => [el, index]);
    stabilizedThis.sort((a, b) => {
      const order = comparator(a[0], b[0]);
      if (order !== 0) return order;
      return a[1] - b[1];
    });
    return stabilizedThis.map((el) => el[0]);
  }

  return (
    <Card
      {...props}
      sx={{
        marginBottom: '25px'
      }}
    >
      <x.div
        display="flex"
        alignItems="center"
        justifyContent="space-between"
        paddingRight="10px"
      >
        <CardHeader title="Credential" />
        <Refresh from="credential" />
      </x.div>
      <Divider />
      <PerfectScrollbar>
        <Box sx={{ minWidth: 800 }}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>
                  <TableSortLabel
                    active={valueToOrderBy === 'key'}
                    direction={
                      valueToOrderBy === 'key' ? orderDirection : 'asc'
                    }
                    onClick={createSortHandler('key')}
                  >
                    Key
                  </TableSortLabel>
                </TableCell>
                <TableCell>Hash</TableCell>
                <TableCell>
                  <TableSortLabel
                    active={valueToOrderBy === 'updatedAt'}
                    direction={
                      valueToOrderBy === 'updatedAt' ? orderDirection : 'asc'
                    }
                    onClick={createSortHandler('updatedAt')}
                  >
                    Updated At
                  </TableSortLabel>
                </TableCell>
                <TableCell>
                  <TableSortLabel
                    active={valueToOrderBy === 'createdAt'}
                    direction={
                      valueToOrderBy === 'createdAt' ? orderDirection : 'asc'
                    }
                    onClick={createSortHandler('createdAt')}
                  >
                    Created At
                  </TableSortLabel>
                </TableCell>
              </TableRow>
            </TableHead>
            <CredentialList
              sortedList={
                credentialList &&
                stableSort(
                  credentialList,
                  getComparator(orderDirection, valueToOrderBy)
                )
              }
            />
          </Table>
        </Box>
      </PerfectScrollbar>
      <Box
        sx={{
          display: 'flex',
          justifyContent: 'flex-end',
          p: 2
        }}
      >
        <RouterLink to="/app/credentials">
          <Button
            color="primary"
            endIcon={<ArrowRightIcon />}
            size="small"
            variant="text"
          >
            View all
          </Button>
        </RouterLink>
      </Box>
    </Card>
  );
};

export default CredentialOverview;
