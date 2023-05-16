/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. Licensed under a proprietary license.
 * See the License.txt file for more information. You may not use this file
 * except in compliance with the proprietary license.
 */

import styled from 'styled-components';
import {INCIDENT_WRAPPER_HEADER_HEIGHT} from 'modules/constants';

const Overlay = styled.div`
  width: 100%;
  height: 60%;
  position: absolute;
  background-color: var(--cds-layer-01);
  border-bottom: 1px solid var(--cds-border-subtle-01);
  display: grid;
  grid-template-rows: ${INCIDENT_WRAPPER_HEADER_HEIGHT} 1fr;
`;

export {Overlay};
